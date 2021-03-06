package log_streamer

import (
	"sync"
	"unicode/utf8"

	"github.com/cloudfoundry/dropsonde/logs"
	"github.com/cloudfoundry/sonde-go/events"
)

type streamDestination struct {
	guid        string
	sourceName  string
	sourceId    string
	messageType events.LogMessage_MessageType
	buffer      []byte
	bufferLock  sync.Mutex
}

func newStreamDestination(guid, sourceName, sourceId string, messageType events.LogMessage_MessageType) *streamDestination {
	return &streamDestination{
		guid:        guid,
		sourceName:  sourceName,
		sourceId:    sourceId,
		messageType: messageType,
		buffer:      make([]byte, 0, MAX_MESSAGE_SIZE),
	}
}

func (destination *streamDestination) Write(data []byte) (int, error) {
	destination.processMessage(string(data))
	return len(data), nil
}

func (destination *streamDestination) flush() {
	msg := destination.copyAndResetBuffer()

	if len(msg) > 0 {
		switch destination.messageType {
		case events.LogMessage_OUT:
			logs.SendAppLog(destination.guid, string(msg), destination.sourceName, destination.sourceId)
		case events.LogMessage_ERR:
			logs.SendAppErrorLog(destination.guid, string(msg), destination.sourceName, destination.sourceId)
		}
	}
}

func (destination *streamDestination) copyAndResetBuffer() []byte {
	destination.bufferLock.Lock()
	defer destination.bufferLock.Unlock()

	if len(destination.buffer) > 0 {
		msg := make([]byte, len(destination.buffer))
		copy(msg, destination.buffer)

		destination.buffer = destination.buffer[:0]
		return msg
	}

	return []byte{}
}

func (destination *streamDestination) processMessage(message string) {
	start := 0
	for i, rune := range message {
		if rune == '\n' || rune == '\r' {
			destination.processString(message[start:i], true)
			start = i + 1
		}
	}

	if start < len(message) {
		destination.processString(message[start:], false)
	}
}

func (destination *streamDestination) processString(message string, terminates bool) {
	for {
		message = destination.appendToBuffer(message)
		if len(message) == 0 {
			break
		}
		destination.flush()
	}

	if terminates {
		destination.flush()
	}
}

func (destination *streamDestination) appendToBuffer(message string) string {
	destination.bufferLock.Lock()
	defer destination.bufferLock.Unlock()

	if len(message)+len(destination.buffer) >= MAX_MESSAGE_SIZE {
		remainingSpaceInBuffer := MAX_MESSAGE_SIZE - len(destination.buffer)
		destination.buffer = append(destination.buffer, []byte(message[0:remainingSpaceInBuffer])...)

		r, _ := utf8.DecodeLastRune(destination.buffer)
		if r == utf8.RuneError {
			destination.buffer = destination.buffer[0 : len(destination.buffer)-1]
			message = message[remainingSpaceInBuffer-1:]
		} else {
			message = message[remainingSpaceInBuffer:]
		}

		return message
	}

	destination.buffer = append(destination.buffer, []byte(message)...)
	return ""
}

func (d *streamDestination) withSource(sourceName string) *streamDestination {
	return newStreamDestination(d.guid, sourceName, d.sourceId, d.messageType)
}
