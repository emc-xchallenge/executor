package containerstore

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/executor/depot/event"
	"github.com/cloudfoundry-incubator/executor/depot/log_streamer"
	"github.com/cloudfoundry-incubator/executor/depot/steps"
	"github.com/cloudfoundry-incubator/executor/depot/transformer"
	"github.com/cloudfoundry-incubator/garden"
	"github.com/cloudfoundry/gunk/workpool"
	"github.com/pivotal-golang/clock"
	"github.com/pivotal-golang/lager"
)

const (
	tagPropertyPrefix      = "tag:"
	executorPropertyPrefix = "executor:"

	ContainerOwnerProperty         = executorPropertyPrefix + "owner"
	ContainerStateProperty         = executorPropertyPrefix + "state"
	ContainerAllocatedAtProperty   = executorPropertyPrefix + "allocated-at"
	ContainerRootfsProperty        = executorPropertyPrefix + "rootfs"
	ContainerLogProperty           = executorPropertyPrefix + "log-config"
	ContainerMetricsConfigProperty = executorPropertyPrefix + "metrics-config"
	ContainerResultProperty        = executorPropertyPrefix + "result"
	ContainerMemoryMBProperty      = executorPropertyPrefix + "memory-mb"
	ContainerDiskMBProperty        = executorPropertyPrefix + "disk-mb"
	ContainerCPUWeightProperty     = executorPropertyPrefix + "cpu-weight"
	ContainerStartTimeoutProperty  = executorPropertyPrefix + "start-timeout"
)

type ContainerStore interface {
	// Lifecycle
	Reserve(logger lager.Logger, req *executor.AllocationRequest) (executor.Container, error)
	Initialize(logger lager.Logger, req *executor.RunRequest) error
	Create(logger lager.Logger, guid string) (executor.Container, error)
	Run(logger lager.Logger, guid string) error
	Fail(logger lager.Logger, guid string, reason string) (executor.Container, error)
	Stop(logger lager.Logger, guid string) error
	// Destroy(logger lager.Logger, guid string) error

	// // Getters
	// List(logger lager.Logger, tags executor.Tags) ([]executor.Container, error)
	Get(logger lager.Logger, guid string) (executor.Container, error)
	// Metrics(logger lager.Logger, guid []string) (map[string]executor.ContainerMetrics, error)

	// // Hopefully on container?
	// GetFiles(logger lager.Logger, guid, sourcePath string) (io.ReadCloser, error)
}

type containerStore struct {
	ownerName                   string
	iNodeLimit                  uint64
	maxCPUShares                uint64
	healthyMonitoringInterval   time.Duration
	unhealthyMonitoringInterval time.Duration
	healthCheckWorkPool         *workpool.WorkPool

	gardenClient garden.Client
	transformer  transformer.Transformer

	containers     map[string]executor.Container
	containersLock sync.RWMutex

	runningActions     map[string]steps.Step
	runningActionsLock sync.Mutex

	eventEmitter event.Hub

	clock clock.Clock
}

func New(
	ownerName string,
	iNodeLimit uint64,
	maxCPUShares uint64,
	healthyMonitoringInterval time.Duration,
	unhealthyMonitoringInterval time.Duration,
	healthCheckWorkPool *workpool.WorkPool,
	gardenClient garden.Client,
	clock clock.Clock,
	eventEmitter event.Hub,
	transformer transformer.Transformer,
) ContainerStore {
	return &containerStore{
		ownerName:                   ownerName,
		iNodeLimit:                  iNodeLimit,
		maxCPUShares:                maxCPUShares,
		healthyMonitoringInterval:   healthyMonitoringInterval,
		unhealthyMonitoringInterval: unhealthyMonitoringInterval,
		healthCheckWorkPool:         healthCheckWorkPool,
		gardenClient:                gardenClient,
		containers:                  map[string]executor.Container{},
		runningActions:              map[string]steps.Step{},
		eventEmitter:                eventEmitter,
		transformer:                 transformer,
		clock:                       clock,
	}
}

func (cs *containerStore) Reserve(logger lager.Logger, req *executor.AllocationRequest) (executor.Container, error) {
	container := executor.NewReservedContainerFromAllocationRequest(req, time.Now().Unix())

	cs.containersLock.Lock()
	defer cs.containersLock.Unlock()
	_, err := cs.get(logger, container.Guid)
	if err == nil {
		return executor.Container{}, executor.ErrContainerGuidNotAvailable
	}
	cs.containers[container.Guid] = container

	go cs.eventEmitter.Emit(executor.NewContainerReservedEvent(container))

	return container, nil
}

func (cs *containerStore) Initialize(logger lager.Logger, req *executor.RunRequest) error {
	cs.containersLock.Lock()
	defer cs.containersLock.Unlock()

	container, err := cs.get(logger, req.Guid)
	if err != nil {
		return err
	}

	if container.State != executor.StateReserved {
		return executor.ErrInvalidTransition
	}

	container.State = executor.StateInitializing
	container.RunInfo = req.RunInfo
	container.Tags.Add(req.Tags)
	cs.containers[container.Guid] = container

	return nil
}

func (cs *containerStore) Create(logger lager.Logger, guid string) (executor.Container, error) {
	logger = logger.Session("container-store.create")

	cs.containersLock.Lock()
	defer cs.containersLock.Unlock()

	logger.Debug("obtaining-container")
	container, err := cs.get(logger, guid)
	if err != nil {
		logger.Error("failed-to-get-container", err)
		return executor.Container{}, err
	}

	if container.State != executor.StateInitializing {
		return executor.Container{}, executor.ErrInvalidTransition
	}

	container.State = executor.StateCreated

	logStreamer := logStreamerFromContainer(container)
	fmt.Fprintf(logStreamer.Stdout(), "Creating container\n")
	container, err = cs.createInGarden(logger, container)
	if err != nil {
		logger.Error("failed-to-create-container", err)
		fmt.Fprintf(logStreamer.Stderr(), "Failed to create container\n")
		return container, err
	}
	fmt.Fprintf(logStreamer.Stdout(), "Successfully created container\n")

	cs.containers[container.Guid] = container

	return container, nil
}

func (cs *containerStore) createInGarden(logger lager.Logger, container executor.Container) (executor.Container, error) {
	diskScope := garden.DiskLimitScopeExclusive
	if container.DiskScope == executor.TotalDiskLimit {
		diskScope = garden.DiskLimitScopeTotal
	}

	logProperty, err := json.Marshal(container.LogConfig)
	if err != nil {
		logger.Error("failed-to-marshal-log-config", err)
		return executor.Container{}, err
	}

	metricsProperty, err := json.Marshal(container.MetricsConfig)
	if err != nil {
		logger.Error("failed-to-marshal-metrics-config", err)
		return executor.Container{}, err
	}

	containerSpec := garden.ContainerSpec{
		Handle:     container.Guid,
		Privileged: container.Privileged,
		RootFSPath: container.RootFSPath,
		Limits: garden.Limits{
			Memory: garden.MemoryLimits{
				LimitInBytes: uint64(container.MemoryMB * 1024 * 1024),
			},
			Disk: garden.DiskLimits{
				ByteHard:  uint64(container.DiskMB * 1024 * 1024),
				InodeHard: cs.iNodeLimit,
				Scope:     diskScope,
			},
			CPU: garden.CPULimits{
				LimitInShares: uint64(float64(cs.maxCPUShares) * float64(container.CPUWeight) / 100.0),
			},
		},
		Properties: garden.Properties{
			ContainerOwnerProperty:         cs.ownerName,
			ContainerStateProperty:         string(container.State),
			ContainerAllocatedAtProperty:   fmt.Sprintf("%d", container.AllocatedAt),
			ContainerStartTimeoutProperty:  fmt.Sprintf("%d", container.StartTimeout),
			ContainerRootfsProperty:        container.RootFSPath,
			ContainerLogProperty:           string(logProperty),
			ContainerMetricsConfigProperty: string(metricsProperty),
			ContainerMemoryMBProperty:      fmt.Sprintf("%d", container.MemoryMB),
			ContainerDiskMBProperty:        fmt.Sprintf("%d", container.DiskMB),
			ContainerCPUWeightProperty:     fmt.Sprintf("%d", container.CPUWeight),
		},
	}

	for name, value := range container.Tags {
		containerSpec.Properties[tagPropertyPrefix+name] = value
	}

	for _, envVar := range container.Env {
		containerSpec.Env = append(containerSpec.Env, envVar.Name+"="+envVar.Value)
	}

	netOutRules := []garden.NetOutRule{}
	for _, rule := range container.EgressRules {
		if err := rule.Validate(); err != nil {
			logger.Error("invalid-egress-rule", err)
			return executor.Container{}, err
		}

		netOutRule, err := securityGroupRuleToNetOutRule(rule)
		if err != nil {
			logger.Error("failed-to-convert-to-net-out-rule", err)
			return executor.Container{}, err
		}

		netOutRules = append(netOutRules, netOutRule)
	}

	gardenContainer, err := cs.gardenClient.Create(containerSpec)
	if err != nil {
		logger.Error("failed-to-create-container-in-garden", err)
		return executor.Container{}, err
	}

	for _, rule := range netOutRules {
		err = gardenContainer.NetOut(rule)
		if err != nil {
			destroyErr := cs.gardenClient.Destroy(container.Guid)
			if destroyErr != nil {
				logger.Error("failed-destroy-container", err)
			}
			logger.Error("failed-to-net-out", err)
			return executor.Container{}, err
		}
	}

	if container.Ports != nil {
		actualPortMappings := make([]executor.PortMapping, len(container.Ports))
		for i, portMapping := range container.Ports {
			actualHost, actualContainerPort, err := gardenContainer.NetIn(uint32(portMapping.HostPort), uint32(portMapping.ContainerPort))
			if err != nil {
				logger.Error("failed-to-net-in", err)

				destroyErr := cs.gardenClient.Destroy(container.Guid)
				if destroyErr != nil {
					logger.Error("failed-destroy-container", destroyErr)
				}

				return executor.Container{}, err
			}
			actualPortMappings[i].ContainerPort = uint16(actualContainerPort)
			actualPortMappings[i].HostPort = uint16(actualHost)
		}

		container.Ports = actualPortMappings
	}

	info, err := gardenContainer.Info()
	if err != nil {
		logger.Error("failed-container-info", err)

		destroyErr := cs.gardenClient.Destroy(container.Guid)
		if destroyErr != nil {
			logger.Error("failed-destroy-container", destroyErr)
		}

		return executor.Container{}, err
	}
	container.ExternalIP = info.ExternalIP
	container.GardenContainer = gardenContainer

	return container, nil
}

func (cs *containerStore) Run(logger lager.Logger, guid string) error {
	logger = logger.Session("container-store.run")

	cs.containersLock.Lock()
	defer cs.containersLock.Unlock()

	logger.Debug("getting-container")
	container, err := cs.get(logger, guid)
	if err != nil {
		logger.Error("failed-to-get-container", err)
		return err
	}

	if container.State != executor.StateCreated {
		return executor.ErrInvalidTransition
	}

	logStreamer := logStreamerFromContainer(container)

	action, healthCheckPassed, err := cs.transformer.StepsForContainer(logger, container, logStreamer)
	if err != nil {
		return err
	}

	go func() {
		resultCh := make(chan error)
		go func() {
			resultCh <- action.Perform()
		}()

		for {
			select {
			case err := <-resultCh:
				var failed bool
				var failureReason string

				if err != nil {
					failed = true
					failureReason = err.Error()
				}

				cs.containersLock.Lock()
				container = cs.complete(logger, container, failed, failureReason)
				cs.containersLock.Unlock()

				return

			case <-healthCheckPassed:
				cs.containersLock.Lock()
				container.State = executor.StateRunning
				cs.containers[guid] = container
				cs.containersLock.Unlock()
			}
		}
	}()

	cs.runningActionsLock.Lock()
	defer cs.runningActionsLock.Unlock()
	cs.runningActions[container.Guid] = action

	return nil
}

func (cs *containerStore) Fail(logger lager.Logger, guid, reason string) (executor.Container, error) {
	logger = logger.Session("container-store.fail")
	cs.containersLock.Lock()
	defer cs.containersLock.Unlock()

	container, err := cs.get(logger, guid)
	if err != nil {
		logger.Error("failed-to-get-container", err)
		return executor.Container{}, err
	}

	if container.State == executor.StateCompleted {
		logger.Error("invalid-transition", executor.ErrInvalidTransition)
		return executor.Container{}, executor.ErrInvalidTransition
	}

	container = cs.complete(logger, container, true, reason)

	return container, nil
}

func (cs *containerStore) complete(logger lager.Logger, container executor.Container, failed bool, failureReason string) executor.Container {
	container.RunResult = executor.ContainerRunResult{
		Failed:        failed,
		FailureReason: failureReason,
	}

	container.State = executor.StateCompleted
	cs.containers[container.Guid] = container

	go cs.eventEmitter.Emit(executor.NewContainerCompleteEvent(container))

	return container
}

func (cs *containerStore) Stop(logger lager.Logger, guid string) error {
	cs.containersLock.Lock()
	defer cs.containersLock.Unlock()

	container, err := cs.get(logger, guid)
	if err != nil {
		return err
	}

	cs.runningActionsLock.Lock()
	defer cs.runningActionsLock.Unlock()
	action, _ := cs.runningActions[container.Guid]
	action.Cancel()

	return nil
}

func (cs *containerStore) Get(logger lager.Logger, guid string) (executor.Container, error) {
	cs.containersLock.RLock()
	defer cs.containersLock.RUnlock()

	return cs.get(logger, guid)
}

func (cs *containerStore) get(logger lager.Logger, guid string) (executor.Container, error) {
	container, ok := cs.containers[guid]
	if !ok {
		return executor.Container{}, executor.ErrContainerNotFound
	}
	return container, nil
}

func logStreamerFromContainer(container executor.Container) log_streamer.LogStreamer {
	return log_streamer.New(
		container.LogConfig.Guid,
		container.LogConfig.SourceName,
		container.LogConfig.Index,
	)
}
