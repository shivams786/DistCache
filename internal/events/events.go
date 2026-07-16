package events

import (
	"sync"
	"time"
)

type Event struct {
	Time    time.Time         `json:"time"`
	Level   string            `json:"level"`
	Type    string            `json:"type"`
	Message string            `json:"message"`
	Fields  map[string]string `json:"fields,omitempty"`
}

type Log struct {
	mu     sync.RWMutex
	limit  int
	events []Event
}

func New(limit int) *Log {
	if limit <= 0 {
		limit = 100
	}
	return &Log{limit: limit}
}

func (l *Log) Add(level, eventType, message string, fields map[string]string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	event := Event{
		Time:    time.Now().UTC(),
		Level:   level,
		Type:    eventType,
		Message: message,
		Fields:  fields,
	}
	l.events = append(l.events, event)
	if len(l.events) > l.limit {
		copy(l.events, l.events[len(l.events)-l.limit:])
		l.events = l.events[:l.limit]
	}
}

func (l *Log) List() []Event {
	l.mu.RLock()
	defer l.mu.RUnlock()

	out := make([]Event, len(l.events))
	for i := range l.events {
		out[len(l.events)-1-i] = l.events[i]
	}
	return out
}
