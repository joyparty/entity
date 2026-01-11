package cache

import (
	"context"
	"errors"
	"time"

	"github.com/joyparty/entity"
	"github.com/redis/go-redis/v9"
)

type redisCache struct {
	Redis redis.Cmdable
}

// NewRedisCache creates a Redis-backed cache instance for entity data.
func NewRedisCache(client redis.Cmdable) entity.Cacher {
	return &redisCache{Redis: client}
}

func (rc *redisCache) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := rc.Redis.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	return data, nil
}

func (rc *redisCache) Put(ctx context.Context, key string, data []byte, expiration time.Duration) error {
	return rc.Redis.Set(ctx, key, data, expiration).Err()
}

func (rc *redisCache) Delete(ctx context.Context, key string) error {
	return rc.Redis.Del(ctx, key).Err()
}
