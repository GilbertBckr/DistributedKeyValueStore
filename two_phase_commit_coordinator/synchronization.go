package twophasecommitcoordinator

import (
	"distributedKeyValue/persistence"
	"log/slog"
	"sync"
)

// ChannelManager handles the thread-safe map of order IDs to channels
type ChannelManager struct {
	mu    sync.RWMutex
	chans map[string]chan<- persistence.TransactionCoordinatorState
}

func NewChannelManager() *ChannelManager {
	return &ChannelManager{
		chans: make(map[string]chan<- persistence.TransactionCoordinatorState),
	}
}

// Add registers a new buffered channel for a specific order ID
func (m *ChannelManager) Add(transactionId string, ch chan persistence.TransactionCoordinatorState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.chans[transactionId] = ch
}

// Remove safely deletes the channel from the map (used in defer)
func (m *ChannelManager) Remove(transactionId string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.chans, transactionId)
}

// Notify checks if a channel exists for the ID and sends the result if it does.
// It uses a Read lock so multiple workers can notify different channels concurrently.
func (m *ChannelManager) NotifyHandler(transactionId string, result persistence.TransactionCoordinatorState) {
	m.mu.RLock()
	ch, exists := m.chans[transactionId]
	m.mu.RUnlock()

	if exists {
		// Non-blocking send (safe because the channel is buffered by 1)
		select {
		case ch <- result:
		default:
			slog.Warn("Failed to send result for transaction %s: channel is full", transactionId)
		}
	}
}
