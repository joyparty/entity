package entity

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// DefaultCacher is the default cache storage instance.
var DefaultCacher Cacher

// Cacheable is an interface for cacheable entity objects.
type Cacheable interface {
	CacheOption() CacheOption
}

// Cacher is an interface for cache data storage.
type Cacher interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, data []byte, expiration time.Duration) error
	Delete(ctx context.Context, key string) error
}

// CacheOption contains cache configuration parameters.
type CacheOption struct {
	Cacher     Cacher
	Key        string
	Expiration time.Duration
	Compress   bool
	// If true, no cache will be generated.
	// This configuration only controls cache generation, not cache reading.
	// Because there is not enough information to make a judgment before data is read.
	Disable bool
	// Some caches constructed elsewhere have field content that is json encoded before entering the cache.
	// These field cache results need to be decoded twice to be used.
	RecursiveDecode []string
}

func loadCache(ctx context.Context, ent Cacheable) (bool, error) {
	opt, err := getCacheOption(ent)
	if err != nil {
		return false, fmt.Errorf("get option, %w", err)
	}

	data, err := opt.Cacher.Get(ctx, opt.Key)
	if err != nil {
		return false, err
	} else if len(data) == 0 {
		return false, nil
	}

	if opt.Compress {
		zr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return false, fmt.Errorf("uncompress data, %w", err)
		}
		defer zr.Close()

		v, err := io.ReadAll(zr)
		if err != nil {
			return false, fmt.Errorf("uncompress data, %w", err)
		}
		data = v
	}

	if len(opt.RecursiveDecode) > 0 {
		fixed, err := recursiveDecode(data, opt.RecursiveDecode)
		if err != nil {
			return false, fmt.Errorf("recursive decode, %w", err)
		} else if fixed != nil {
			data = fixed
		}
	}

	if err := json.Unmarshal(data, ent); err != nil {
		return false, fmt.Errorf("json decode, %w", err)
	}
	return true, nil
}

// SaveCache saves an entity to the cache.
func SaveCache(ctx context.Context, ent Cacheable) error {
	opt, err := getCacheOption(ent)
	if err != nil {
		return fmt.Errorf("get option, %w", err)
	} else if opt.Disable {
		return nil
	}

	data, err := json.Marshal(ent)
	if err != nil {
		return fmt.Errorf("json encode, %w", err)
	}

	if opt.Compress {
		var zdata bytes.Buffer
		zw := gzip.NewWriter(&zdata)
		if _, err := zw.Write(data); err != nil {
			return fmt.Errorf("compress cache, %w", err)
		}
		if err := zw.Close(); err != nil {
			return fmt.Errorf("comporess cache, %w", err)
		}
		data = zdata.Bytes()
	}

	return opt.Cacher.Put(ctx, opt.Key, data, opt.Expiration)
}

// DeleteCache removes an entity from the cache.
func DeleteCache(ctx context.Context, ent Cacheable) error {
	opt, err := getCacheOption(ent)
	if err != nil {
		return fmt.Errorf("get option, %w", err)
	}

	return opt.Cacher.Delete(ctx, opt.Key)
}

func getCacheOption(ent Cacheable) (CacheOption, error) {
	opt := ent.CacheOption()

	if opt.Cacher == nil {
		if DefaultCacher == nil {
			return opt, fmt.Errorf("nil default cacher")
		}

		opt.Cacher = DefaultCacher
	}

	if opt.Key == "" {
		return opt, fmt.Errorf("empty cache key")
	}

	if opt.Expiration == 0 {
		opt.Expiration = 5 * time.Minute
	}

	return opt, nil
}

func recursiveDecode(data []byte, keys []string) ([]byte, error) {
	if len(keys) == 0 || len(data) == 0 || data[0] != '{' {
		return nil, nil
	}

	vals := map[string]json.RawMessage{}
	if err := json.Unmarshal(data, &vals); err != nil {
		return nil, err
	}

	fixed := false
	for _, key := range keys {
		if data, ok := vals[key]; ok && len(data) > 0 && data[0] == '"' {
			fixed = true

			var s string
			if err := json.Unmarshal(data, &s); err != nil {
				return nil, err
			}

			vals[key] = []byte(s)
		}
	}

	if !fixed {
		return nil, nil
	}

	encoded, err := json.Marshal(vals)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}
