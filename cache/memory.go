package cache

import (
	"context"
	"time"

	"github.com/joyparty/entity"
	"github.com/patrickmn/go-cache"
)

type memoryCache struct {
	values *cache.Cache
}

// NewMemoryCache creates a new in-memory cache instance.
func NewMemoryCache() entity.Cacher {
	return &memoryCache{
		values: cache.New(5*time.Minute, 10*time.Minute),
	}
}

func (mc *memoryCache) Get(_ context.Context, key string) ([]byte, error) {
	if v, ok := mc.values.Get(key); ok {
		return v.([]byte), nil
	}
	return nil, nil
}

func (mc *memoryCache) Put(_ context.Context, key string, data []byte, expiration time.Duration) error {
	mc.values.Set(key, data, expiration)
	return nil
}

func (mc *memoryCache) Delete(_ context.Context, key string) error {
	mc.values.Delete(key)
	return nil
}
