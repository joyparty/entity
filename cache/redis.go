package cache

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/joyparty/entity"
)

type redisCache struct {
	Redis redis.Cmdable
}

// NewRedisCache 使用redis缓存实体数据
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
