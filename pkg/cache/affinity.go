package cache

import (
	"sync"
	"time"
)

type entry struct {
	profileID string
	expiresAt time.Time
}

type Affinity struct {
	mu   sync.RWMutex
	data map[string]entry
	ttl  time.Duration
}

func NewAffinity(ttl time.Duration) *Affinity {
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	return &Affinity{data: map[string]entry{}, ttl: ttl}
}

func (a *Affinity) Set(source, profileID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.data[source] = entry{profileID: profileID, expiresAt: time.Now().Add(a.ttl)}
}

func (a *Affinity) Get(source string) (string, bool) {
	a.mu.RLock()
	e, ok := a.data[source]
	a.mu.RUnlock()
	if !ok {
		return "", false
	}
	if time.Now().After(e.expiresAt) {
		a.Delete(source)
		return "", false
	}
	return e.profileID, true
}

func (a *Affinity) Delete(source string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.data, source)
}
