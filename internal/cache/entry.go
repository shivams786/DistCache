package cache

import "time"

type Entry struct {
	Key        string    `json:"key"`
	Value      []byte    `json:"value"`
	ExpiresAt  time.Time `json:"expires_at,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	AccessedAt time.Time `json:"accessed_at"`
	HitCount   int64     `json:"hit_count"`
}

func (e Entry) Expired(now time.Time) bool {
	return !e.ExpiresAt.IsZero() && !e.ExpiresAt.After(now)
}

func (e Entry) Clone() Entry {
	copied := e
	if e.Value != nil {
		copied.Value = append([]byte(nil), e.Value...)
	}
	return copied
}
