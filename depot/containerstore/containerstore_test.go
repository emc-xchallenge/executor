package containerstore_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cloudfoundry/gunk/workpool"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/cloudfoundry-incubator/bbs/models"
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/executor/depot/containerstore"
	"github.com/cloudfoundry-incubator/executor/depot/transformer/faketransformer"
	"github.com/cloudfoundry-incubator/garden"
	"github.com/pivotal-golang/clock/fakeclock"
	"github.com/pivotal-golang/lager/lagertest"

	eventfakes "github.com/cloudfoundry-incubator/executor/depot/event/fakes"
	stepfakes "github.com/cloudfoundry-incubator/executor/depot/steps/fakes"
	gfakes "github.com/cloudfoundry-incubator/garden/fakes"
)

var _ = Describe("Container Store", func() {
	var (
		containerStore containerstore.ContainerStore

		iNodeLimit   uint64
		maxCPUShares uint64
		ownerName    string

		gardenClient    *gfakes.FakeClient
		gardenContainer *gfakes.FakeContainer
		megatron        *faketransformer.FakeTransformer

		clock        *fakeclock.FakeClock
		eventEmitter *eventfakes.FakeHub

		logger *lagertest.TestLogger
	)

	BeforeEach(func() {
		gardenClient = &gfakes.FakeClient{}
		clock = fakeclock.NewFakeClock(time.Now())
		eventEmitter = &eventfakes.FakeHub{}
		logger = lagertest.NewTestLogger("test-container-store")

		iNodeLimit = 64
		maxCPUShares = 100
		ownerName = "test-owner"

		healthyMonitoringInterval := 5 * time.Millisecond
		unhealthyMonitoringInterval := 10 * time.Millisecond
		healthCheckWorkPool, err := workpool.NewWorkPool(1)
		Expect(err).NotTo(HaveOccurred())

		megatron = &faketransformer.FakeTransformer{}
		containerStore = containerstore.New(
			ownerName,
			iNodeLimit,
			maxCPUShares,
			healthyMonitoringInterval,
			unhealthyMonitoringInterval,
			healthCheckWorkPool,
			gardenClient,
			clock,
			eventEmitter,
			megatron,
		)
	})

	Describe("Reserve", func() {
		var (
			containerGuid     string
			containerTags     executor.Tags
			containerResource executor.Resource
			req               *executor.AllocationRequest
		)

		BeforeEach(func() {
			containerGuid = "container-guid"
			containerTags = executor.Tags{
				"Foo": "bar",
			}
			containerResource = executor.Resource{
				MemoryMB:   1024,
				DiskMB:     1024,
				RootFSPath: "/foo/bar",
			}
			req = &executor.AllocationRequest{
				Guid:     containerGuid,
				Tags:     containerTags,
				Resource: containerResource,
			}
		})

		It("returns a populated container", func() {
			container, err := containerStore.Reserve(logger, req)
			Expect(err).NotTo(HaveOccurred())

			Expect(container.Guid).To(Equal(containerGuid))
			Expect(container.Tags).To(Equal(containerTags))
			Expect(container.Resource).To(Equal(containerResource))
			Expect(container.State).To(Equal(executor.StateReserved))
			Expect(container.AllocatedAt).To(Equal(time.Now().Unix()))
		})

		It("tracks the container", func() {
			container, err := containerStore.Reserve(logger, req)
			Expect(err).NotTo(HaveOccurred())

			found, err := containerStore.Get(logger, container.Guid)
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(Equal(container))
		})

		It("emits a reserved container event", func() {
			container, err := containerStore.Reserve(logger, req)
			Expect(err).NotTo(HaveOccurred())

			Eventually(eventEmitter.EmitCallCount).Should(Equal(1))

			event := eventEmitter.EmitArgsForCall(0)
			Expect(event).To(Equal(executor.ContainerReservedEvent{
				RawContainer: container,
			}))
		})

		Context("when the container guid is already reserved", func() {
			BeforeEach(func() {
				_, err := containerStore.Reserve(logger, req)
				Expect(err).NotTo(HaveOccurred())
			})

			It("fails with container guid not available", func() {
				_, err := containerStore.Reserve(logger, req)
				Expect(err).To(Equal(executor.ErrContainerGuidNotAvailable))
			})
		})
	})

	Describe("Initialize", func() {
		var (
			containerGuid string

			req     *executor.RunRequest
			runInfo executor.RunInfo
			runTags executor.Tags
		)

		BeforeEach(func() {
			containerGuid = "container-guid"
			runInfo = executor.RunInfo{
				CPUWeight:    2,
				StartTimeout: 50,
				Privileged:   true,
			}

			runTags = executor.Tags{
				"Beep": "Boop",
			}

			req = &executor.RunRequest{
				Guid:    containerGuid,
				RunInfo: runInfo,
				Tags:    runTags,
			}
		})

		Context("when the container has not been reserved", func() {
			It("returns a container not found error", func() {
				err := containerStore.Initialize(logger, req)
				Expect(err).To(Equal(executor.ErrContainerNotFound))
			})
		})

		Context("when the conatiner is reserved", func() {
			BeforeEach(func() {
				allocationReq := &executor.AllocationRequest{
					Guid: containerGuid,
					Tags: executor.Tags{},
				}

				_, err := containerStore.Reserve(logger, allocationReq)
				Expect(err).NotTo(HaveOccurred())
			})

			It("populates the container with info from the run request", func() {
				err := containerStore.Initialize(logger, req)
				Expect(err).NotTo(HaveOccurred())

				container, err := containerStore.Get(logger, req.Guid)
				Expect(err).NotTo(HaveOccurred())
				Expect(container.State).To(Equal(executor.StateInitializing))
				Expect(container.RunInfo).To(Equal(runInfo))
				Expect(container.Tags).To(Equal(runTags))
			})
		})

		Context("when the container exists but is not reserved", func() {
			BeforeEach(func() {
				allocationReq := &executor.AllocationRequest{
					Guid: containerGuid,
					Tags: executor.Tags{},
				}

				_, err := containerStore.Reserve(logger, allocationReq)
				Expect(err).NotTo(HaveOccurred())

				err = containerStore.Initialize(logger, req)
				Expect(err).NotTo(HaveOccurred())
			})

			It("returns an invalid state tranistion error", func() {
				err := containerStore.Initialize(logger, req)
				Expect(err).To(Equal(executor.ErrInvalidTransition))
			})
		})
	})

	Describe("Create", func() {
		var (
			containerGuid string

			resource      executor.Resource
			allocationReq *executor.AllocationRequest
		)

		BeforeEach(func() {
			containerGuid = "container-guid"
			resource = executor.Resource{
				MemoryMB:   1024,
				DiskMB:     1024,
				RootFSPath: "/foo/bar",
			}

			allocationReq = &executor.AllocationRequest{
				Guid: containerGuid,
				Tags: executor.Tags{
					"Foo": "Bar",
				},
				Resource: resource,
			}
		})

		Context("when the container is initializing", func() {
			var (
				externalIP string
				runReq     *executor.RunRequest
			)

			BeforeEach(func() {
				externalIP = "6.6.6.6"
				env := []executor.EnvironmentVariable{
					{Name: "foo", Value: "bar"},
					{Name: "beep", Value: "booop"},
				}

				runInfo := executor.RunInfo{
					Privileged:   true,
					CPUWeight:    50,
					StartTimeout: 99,
					LogConfig: executor.LogConfig{
						Guid:       "log-guid",
						Index:      1,
						SourceName: "test-source",
					},
					MetricsConfig: executor.MetricsConfig{
						Guid:  "metric-guid",
						Index: 1,
					},
					Env: env,
				}
				runReq = &executor.RunRequest{
					Guid:    containerGuid,
					RunInfo: runInfo,
				}

				gardenContainer = &gfakes.FakeContainer{}
				gardenContainer.InfoReturns(garden.ContainerInfo{ExternalIP: externalIP}, nil)
				gardenClient.CreateReturns(gardenContainer, nil)
			})

			JustBeforeEach(func() {
				_, err := containerStore.Reserve(logger, allocationReq)
				Expect(err).NotTo(HaveOccurred())

				err = containerStore.Initialize(logger, runReq)
				Expect(err).NotTo(HaveOccurred())
			})

			It("sets the container state to created", func() {
				_, err := containerStore.Create(logger, containerGuid)
				Expect(err).NotTo(HaveOccurred())

				container, err := containerStore.Get(logger, containerGuid)
				Expect(err).NotTo(HaveOccurred())
				Expect(container.State).To(Equal(executor.StateCreated))
			})

			It("creates the container in garden with the correct limits", func() {
				_, err := containerStore.Create(logger, containerGuid)
				Expect(err).NotTo(HaveOccurred())

				Expect(gardenClient.CreateCallCount()).To(Equal(1))
				containerSpec := gardenClient.CreateArgsForCall(0)
				Expect(containerSpec.Handle).To(Equal(containerGuid))
				Expect(containerSpec.RootFSPath).To(Equal(resource.RootFSPath))
				Expect(containerSpec.Privileged).To(Equal(true))

				Expect(containerSpec.Limits.Memory.LimitInBytes).To(BeEquivalentTo(resource.MemoryMB * 1024 * 1024))

				Expect(containerSpec.Limits.Disk.Scope).To(Equal(garden.DiskLimitScopeExclusive))
				Expect(containerSpec.Limits.Disk.ByteHard).To(BeEquivalentTo(resource.DiskMB * 1024 * 1024))
				Expect(containerSpec.Limits.Disk.InodeHard).To(Equal(iNodeLimit))

				expectedCPUShares := uint64(float64(maxCPUShares) * float64(runReq.CPUWeight) / 100.0)
				Expect(containerSpec.Limits.CPU.LimitInShares).To(Equal(expectedCPUShares))
			})

			It("creates the container with the correct properties", func() {
				_, err := containerStore.Create(logger, containerGuid)
				Expect(err).NotTo(HaveOccurred())

				Expect(gardenClient.CreateCallCount()).To(Equal(1))
				containerSpec := gardenClient.CreateArgsForCall(0)

				expectedLogProperty, err := json.Marshal(runReq.LogConfig)
				Expect(err).NotTo(HaveOccurred())

				expectedMetricProperty, err := json.Marshal(runReq.MetricsConfig)
				Expect(err).NotTo(HaveOccurred())

				Expect(containerSpec.Properties).To(Equal(garden.Properties{
					containerstore.ContainerOwnerProperty:         ownerName,
					containerstore.ContainerStateProperty:         string(executor.StateCreated),
					containerstore.ContainerAllocatedAtProperty:   fmt.Sprintf("%d", time.Now().Unix()),
					containerstore.ContainerStartTimeoutProperty:  fmt.Sprintf("%d", runReq.StartTimeout),
					containerstore.ContainerRootfsProperty:        resource.RootFSPath,
					containerstore.ContainerLogProperty:           string(expectedLogProperty),
					containerstore.ContainerMetricsConfigProperty: string(expectedMetricProperty),
					containerstore.ContainerMemoryMBProperty:      fmt.Sprintf("%d", resource.MemoryMB),
					containerstore.ContainerDiskMBProperty:        fmt.Sprintf("%d", resource.DiskMB),
					containerstore.ContainerCPUWeightProperty:     fmt.Sprintf("%d", runReq.CPUWeight),
					"tag:Foo": "Bar",
				}))
			})

			It("creates the container with the correct environment", func() {
				_, err := containerStore.Create(logger, containerGuid)
				Expect(err).NotTo(HaveOccurred())

				Expect(gardenClient.CreateCallCount()).To(Equal(1))
				containerSpec := gardenClient.CreateArgsForCall(0)

				expectedEnv := []string{}
				for _, envVar := range runReq.Env {
					expectedEnv = append(expectedEnv, envVar.Name+"="+envVar.Value)
				}
				Expect(containerSpec.Env).To(Equal(expectedEnv))
			})

			It("sets the correct external ip", func() {
				container, err := containerStore.Create(logger, containerGuid)
				Expect(err).NotTo(HaveOccurred())
				Expect(container.ExternalIP).To(Equal(externalIP))
			})

			Context("when egress rules are requested", func() {

				BeforeEach(func() {
					egressRules := []*models.SecurityGroupRule{
						{
							Protocol:     "icmp",
							Destinations: []string{"1.1.1.1"},
							IcmpInfo: &models.ICMPInfo{
								Type: 2,
								Code: 10,
							},
						},
						{
							Protocol:     "icmp",
							Destinations: []string{"1.1.1.1"},
							IcmpInfo: &models.ICMPInfo{
								Type: 2,
								Code: 10,
							},
						},
					}
					runReq.EgressRules = egressRules
				})

				It("calls NetOut for each egress rule", func() {
					_, err := containerStore.Create(logger, containerGuid)
					Expect(err).NotTo(HaveOccurred())

					Expect(gardenContainer.NetOutCallCount()).To(Equal(2))
				})

				Context("when NetOut fails", func() {
					BeforeEach(func() {
						gardenContainer.NetOutStub = func(garden.NetOutRule) error {
							if gardenContainer.NetOutCallCount() == 1 {
								return nil
							} else {
								return errors.New("failed net out!")
							}
						}
					})

					It("destroys the created container and returns an error", func() {
						_, err := containerStore.Create(logger, containerGuid)
						Expect(err).To(Equal(errors.New("failed net out!")))

						Expect(gardenClient.CreateCallCount()).To(Equal(1))
						Expect(gardenClient.DestroyCallCount()).To(Equal(1))

						Expect(gardenClient.DestroyArgsForCall(0)).To(Equal(containerGuid))
					})
				})

				Context("when a egress rule is not valid", func() {
					BeforeEach(func() {
						egressRule := &models.SecurityGroupRule{
							Protocol: "tcp",
						}
						runReq.EgressRules = append(runReq.EgressRules, egressRule)
					})

					It("returns an error", func() {
						_, err := containerStore.Create(logger, containerGuid)
						Expect(err).To(HaveOccurred())

						Expect(gardenClient.CreateCallCount()).To(Equal(0))
					})
				})
			})

			Context("when ports are requested", func() {
				BeforeEach(func() {
					portMapping := []executor.PortMapping{
						{ContainerPort: 8080},
						{ContainerPort: 9090},
					}
					runReq.Ports = portMapping

					gardenContainer.NetInStub = func(uint32, containerPort uint32) (uint32, uint32, error) {
						switch containerPort {
						case 8080:
							return 16000, 8080, nil
						case 9090:
							return 32000, 9090, nil
						default:
							return 0, 0, errors.New("failed-net-in")
						}
					}
				})

				It("calls NetIn on the container for each port", func() {
					_, err := containerStore.Create(logger, containerGuid)
					Expect(err).NotTo(HaveOccurred())

					Expect(gardenContainer.NetInCallCount()).To(Equal(2))
				})

				It("saves the actual port mappings on the container", func() {
					container, err := containerStore.Create(logger, containerGuid)
					Expect(err).NotTo(HaveOccurred())

					Expect(container.Ports[0].ContainerPort).To(BeEquivalentTo(8080))
					Expect(container.Ports[0].HostPort).To(BeEquivalentTo(16000))
					Expect(container.Ports[1].ContainerPort).To(BeEquivalentTo(9090))
					Expect(container.Ports[1].HostPort).To(BeEquivalentTo(32000))

					fetchedContainer, err := containerStore.Get(logger, containerGuid)
					Expect(err).NotTo(HaveOccurred())
					Expect(fetchedContainer).To(Equal(container))
				})

				Context("when NetIn fails", func() {
					BeforeEach(func() {
						gardenContainer.NetInReturns(0, 0, errors.New("failed generating net in rules"))
					})

					It("destroys the container and returns an error", func() {
						_, err := containerStore.Create(logger, containerGuid)
						Expect(err).To(HaveOccurred())

						Expect(gardenClient.DestroyCallCount()).To(Equal(1))
						Expect(gardenClient.DestroyArgsForCall(0)).To(Equal(containerGuid))
					})
				})
			})

			Context("when a total disk scope is request", func() {
				BeforeEach(func() {
					runReq.DiskScope = executor.TotalDiskLimit
				})

				It("creates the container with the correct disk scope", func() {
					_, err := containerStore.Create(logger, containerGuid)
					Expect(err).NotTo(HaveOccurred())

					Expect(gardenClient.CreateCallCount()).To(Equal(1))
					containerSpec := gardenClient.CreateArgsForCall(0)
					Expect(containerSpec.Limits.Disk.Scope).To(Equal(garden.DiskLimitScopeTotal))
				})
			})

			Context("when creating the container fails", func() {
				BeforeEach(func() {
					gardenClient.CreateReturns(nil, errors.New("boom!"))
				})

				It("returns an error", func() {
					_, err := containerStore.Create(logger, containerGuid)
					Expect(err).To(Equal(errors.New("boom!")))
				})
			})

			Context("when requesting the external IP for the created fails", func() {
				BeforeEach(func() {
					gardenContainer.InfoReturns(garden.ContainerInfo{}, errors.New("could not obtain info"))
				})

				It("returns an error", func() {
					_, err := containerStore.Create(logger, containerGuid)
					Expect(err).To(HaveOccurred())

					Expect(gardenClient.DestroyCallCount()).To(Equal(1))
					Expect(gardenClient.DestroyArgsForCall(0)).To(Equal(containerGuid))
				})
			})
		})

		Context("when the container does not exist", func() {
			It("returns a conatiner not found error", func() {
				_, err := containerStore.Create(logger, "bogus-guid")
				Expect(err).To(Equal(executor.ErrContainerNotFound))
			})
		})

		Context("when the container is not initializing", func() {
			BeforeEach(func() {
				_, err := containerStore.Reserve(logger, allocationReq)
				Expect(err).NotTo(HaveOccurred())
			})

			It("returns an invalid state transition error", func() {
				_, err := containerStore.Create(logger, containerGuid)
				Expect(err).To(Equal(executor.ErrInvalidTransition))
			})
		})
	})

	Describe("Run", func() {
		var (
			containerGuid string
			allocationReq *executor.AllocationRequest
			runProcess    *gfakes.FakeProcess
		)

		BeforeEach(func() {
			containerGuid = "container-guid"

			allocationReq = &executor.AllocationRequest{
				Guid: containerGuid,
			}

			gardenContainer = &gfakes.FakeContainer{}
			runProcess = &gfakes.FakeProcess{}

			gardenContainer.RunReturns(runProcess, nil)
			gardenClient.CreateReturns(gardenContainer, nil)
			gardenClient.LookupReturns(gardenContainer, nil)
		})

		Context("when it is in the created state", func() {
			var (
				runReq            *executor.RunRequest
				actionStep        *stepfakes.FakeStep
				healthCheckPassed chan struct{}
				performResult     chan error
			)

			BeforeEach(func() {
				runAction := &models.Action{
					RunAction: &models.RunAction{
						Path: "/foo/bar",
					},
				}

				runReq = &executor.RunRequest{
					Guid: containerGuid,
					RunInfo: executor.RunInfo{
						Action: runAction,
					},
				}

				actionStep = &stepfakes.FakeStep{}
				healthCheckPassed = make(chan struct{})
				performResult = make(chan error, 1)

				megatron.StepsForContainerReturns(actionStep, healthCheckPassed, nil)
				actionStep.PerformStub = func() error {
					result := <-performResult
					return result
				}
			})

			JustBeforeEach(func() {
				_, err := containerStore.Reserve(logger, allocationReq)
				Expect(err).NotTo(HaveOccurred())

				err = containerStore.Initialize(logger, runReq)
				Expect(err).NotTo(HaveOccurred())

				_, err = containerStore.Create(logger, containerGuid)
				Expect(err).NotTo(HaveOccurred())
			})

			It("performs the step", func() {
				err := containerStore.Run(logger, containerGuid)
				Expect(err).NotTo(HaveOccurred())

				Expect(megatron.StepsForContainerCallCount()).To(Equal(1))
				Eventually(actionStep.PerformCallCount).Should(Equal(1))
			})

			It("sets the container state to running once the healthcheck passes", func() {
				err := containerStore.Run(logger, containerGuid)
				Expect(err).NotTo(HaveOccurred())

				container, err := containerStore.Get(logger, containerGuid)
				Expect(err).NotTo(HaveOccurred())
				Expect(container.State).To(Equal(executor.StateCreated))

				healthCheckPassed <- struct{}{}
				Eventually(func() executor.State {
					container, err := containerStore.Get(logger, containerGuid)
					Expect(err).NotTo(HaveOccurred())
					return container.State
				}).Should(Equal(executor.StateRunning))
			})

			Context("when the action exits", func() {
				var result error

				JustBeforeEach(func() {
					performResult <- result
				})

				It("sets its state to completed", func() {
					err := containerStore.Run(logger, containerGuid)
					Expect(err).NotTo(HaveOccurred())

					Eventually(actionStep.PerformCallCount).Should(Equal(1))

					container, err := containerStore.Get(logger, containerGuid)
					Expect(err).NotTo(HaveOccurred())
					Expect(container.State).To(Equal(executor.StateCompleted))
				})

				It("emits a container completed event", func() {
					err := containerStore.Run(logger, containerGuid)
					Expect(err).NotTo(HaveOccurred())

					Eventually(eventEmitter.EmitCallCount).Should(Equal(2))

					container, err := containerStore.Get(logger, containerGuid)
					Expect(err).NotTo(HaveOccurred())

					event := eventEmitter.EmitArgsForCall(1)
					Expect(event).To(Equal(executor.ContainerCompleteEvent{
						RawContainer: container,
					}))
				})

				Context("successfully", func() {
					It("sets the result on the container", func() {
						err := containerStore.Run(logger, containerGuid)
						Expect(err).NotTo(HaveOccurred())

						Eventually(actionStep.PerformCallCount).Should(Equal(1))

						container, err := containerStore.Get(logger, containerGuid)
						Expect(err).NotTo(HaveOccurred())
						Expect(container.RunResult.Failed).To(Equal(false))
						Expect(container.RunResult.Stopped).To(Equal(false))
					})
				})

				Context("unsuccessfully", func() {
					BeforeEach(func() {
						result = errors.New("BOOOOOOM!!!!!!!")
					})

					It("sets the run result on the container", func() {
						err := containerStore.Run(logger, containerGuid)
						Expect(err).NotTo(HaveOccurred())

						Eventually(actionStep.PerformCallCount).Should(Equal(1))

						container, err := containerStore.Get(logger, containerGuid)
						Expect(err).NotTo(HaveOccurred())
						Expect(container.RunResult.Failed).To(Equal(true))
						Expect(container.RunResult.FailureReason).To(Equal("BOOOOOOM!!!!!!!"))
						Expect(container.RunResult.Stopped).To(Equal(false))
					})
				})
			})

			Context("when the transformer fails to generate steps", func() {
				BeforeEach(func() {
					megatron.StepsForContainerReturns(nil, nil, errors.New("defeated by the auto bots"))
				})

				It("returns an error", func() {
					err := containerStore.Run(logger, containerGuid)
					Expect(err).To(HaveOccurred())
				})
			})
		})

		Context("when the container does not exist", func() {
			It("returns an ErrContainerNotFound error", func() {
				err := containerStore.Run(logger, containerGuid)
				Expect(err).To(Equal(executor.ErrContainerNotFound))
			})
		})

		Context("When the container is not in the created state", func() {
			JustBeforeEach(func() {
				_, err := containerStore.Reserve(logger, allocationReq)
				Expect(err).NotTo(HaveOccurred())
			})

			It("returns a transition error", func() {
				err := containerStore.Run(logger, containerGuid)
				Expect(err).To(Equal(executor.ErrInvalidTransition))
			})
		})
	})

	Describe("Fail", func() {
		var (
			containerGuid, containerFailureReason string
		)

		BeforeEach(func() {
			containerGuid = "container-guid"
			containerFailureReason = "error creating container"

		})

		JustBeforeEach(func() {
			_, err := containerStore.Reserve(logger, &executor.AllocationRequest{Guid: containerGuid})
			Expect(err).NotTo(HaveOccurred())
		})

		It("sets the garden container state to completed", func() {
			container, err := containerStore.Fail(logger, containerGuid, containerFailureReason)
			Expect(err).NotTo(HaveOccurred())
			Expect(container.State).To(Equal(executor.StateCompleted))

			fetchedContainer, err := containerStore.Get(logger, containerGuid)
			Expect(err).NotTo(HaveOccurred())
			Expect(fetchedContainer).To(Equal(container))
		})

		It("sets the container result", func() {
			container, err := containerStore.Fail(logger, containerGuid, containerFailureReason)
			Expect(err).NotTo(HaveOccurred())
			Expect(container.RunResult.Failed).To(BeTrue())
			Expect(container.RunResult.FailureReason).To(Equal(containerFailureReason))

			fetchedContainer, err := containerStore.Get(logger, containerGuid)
			Expect(err).NotTo(HaveOccurred())
			Expect(fetchedContainer).To(Equal(container))
		})

		It("emits a container complete event", func() {
			container, err := containerStore.Fail(logger, containerGuid, containerFailureReason)
			Expect(err).NotTo(HaveOccurred())

			Eventually(eventEmitter.EmitCallCount).Should(Equal(2))
			event := eventEmitter.EmitArgsForCall(1)
			Expect(event).To(Equal(executor.ContainerCompleteEvent{RawContainer: container}))
		})

		Context("when the container is already completed", func() {
			JustBeforeEach(func() {
				_, err := containerStore.Fail(logger, containerGuid, containerFailureReason)
				Expect(err).NotTo(HaveOccurred())
			})

			It("returns an ErrInvalidTransition", func() {
				_, err := containerStore.Fail(logger, containerGuid, containerFailureReason)
				Expect(err).To(Equal(executor.ErrInvalidTransition))
			})
		})

		Context("when the container does not exist", func() {
			It("returns a ErrContainerNotFound", func() {
				_, err := containerStore.Fail(logger, "", containerFailureReason)
				Expect(err).To(Equal(executor.ErrContainerNotFound))
			})
		})
	})

	Describe("Stop", func() {
		var (
			containerGuid string
			actionStep    *stepfakes.FakeStep
		)

		BeforeEach(func() {
			containerGuid = "container-guid"
			actionStep = &stepfakes.FakeStep{}

			gardenClient.CreateReturns(gardenContainer, nil)
			megatron.StepsForContainerReturns(actionStep, nil, nil)
		})

		JustBeforeEach(func() {
			_, err := containerStore.Reserve(logger, &executor.AllocationRequest{Guid: containerGuid})
			Expect(err).NotTo(HaveOccurred())

			err = containerStore.Initialize(logger, &executor.RunRequest{Guid: containerGuid})
			Expect(err).NotTo(HaveOccurred())

			_, err = containerStore.Create(logger, containerGuid)
			Expect(err).NotTo(HaveOccurred())

			err = containerStore.Run(logger, containerGuid)
			Expect(err).NotTo(HaveOccurred())
		})

		Context("when the container has processes associated with it", func() {
			It("cancels the running process", func() {
				err := containerStore.Stop(logger, containerGuid)
				Expect(err).NotTo(HaveOccurred())

				Eventually(actionStep.CancelCallCount).Should(Equal(1))
			})
		})

		Context("when the container does not have processes associated with it", func() {
			It("destroys the container", func() {
				Expect("ToDo").To(Equal("NotDone"))
			})
		})

		Context("when the container does not exist", func() {
			It("returns an ErrContainerNotFound", func() {
				err := containerStore.Stop(logger, "")
				Expect(err).To(Equal(executor.ErrContainerNotFound))
			})
		})
	})
})
