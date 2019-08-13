package entity

import (
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

// DefaultCacher 默认缓存存储
var DefaultCacher Cacher

// Cacheable 可缓存实体对象接口
type Cacheable interface {
	CacheOption() *CacheOption
}

// Cacher 缓存数据存储接口
type Cacher interface {
	Get(key string) ([]byte, error)
	Put(key string, data []byte, expiration time.Duration) error
	Delete(key string) error
}

// CacheOption 缓存参数
type CacheOption struct {
	Cacher     Cacher
	Key        string
	Expiration time.Duration
	// PHP那边生成的缓存数据，某些字段被json encode过两次
	// golang这边在使用这个缓存数据之前，需要先检查相应的字段是否已经被encode过
	// 如果存在这种情况，需要先decode一次之后再给golang使用
	AutoDecode []string
}

func loadCache(entity Cacheable) (bool, error) {
	opt, err := getCacheOption(entity)
	if err != nil {
		return false, err
	}

	data, err := opt.Cacher.Get(opt.Key)
	if err != nil {
		return false, errors.WithStack(err)
	} else if len(data) == 0 {
		return false, nil
	}

	if len(opt.AutoDecode) > 0 {
		fixed, err := autoDecode(data, opt.AutoDecode)
		if err != nil {
			return false, errors.WithMessage(err, "auto decode cache")
		}

		if fixed != nil {
			data = fixed
		}
	}

	if err := jsoniter.Unmarshal(data, entity); err != nil {
		return false, errors.WithStack(err)
	}

	return true, nil
}

// SaveCache 保存entity缓存
func SaveCache(entity Cacheable) error {
	data, err := jsoniter.Marshal(entity)
	if err != nil {
		return errors.WithStack(err)
	}

	opt, err := getCacheOption(entity)
	if err != nil {
		return err
	}

	return errors.Wrap(opt.Cacher.Put(opt.Key, data, opt.Expiration), "save entity cache")
}

// DeleteCache 删除entity缓存
func DeleteCache(entity Cacheable) error {
	opt, err := getCacheOption(entity)
	if err != nil {
		return err
	}

	return errors.Wrap(opt.Cacher.Delete(opt.Key), "delete entity cache")
}

func getCacheOption(entity Cacheable) (*CacheOption, error) {
	opt := entity.CacheOption()

	if opt.Cacher == nil {
		if DefaultCacher == nil {
			return nil, errors.New("require cacher")
		}

		opt.Cacher = DefaultCacher
	}

	if opt.Key == "" {
		return nil, errors.New("empty cache key")
	}

	if opt.Expiration == 0 {
		opt.Expiration = 5 * time.Minute
	}

	return opt, nil
}

func autoDecode(data []byte, keys []string) ([]byte, error) {
	if len(keys) == 0 || jsoniter.Get(data).ValueType() != jsoniter.ObjectValue {
		return nil, nil
	}

	vals := map[string]jsoniter.RawMessage{}
	if err := jsoniter.Unmarshal(data, &vals); err != nil {
		return nil, errors.WithStack(err)
	}

	fixed := false
	for _, key := range keys {
		if jsoniter.Get(vals[key]).ValueType() == jsoniter.StringValue {
			fixed = true

			var s string
			if err := jsoniter.Unmarshal(vals[key], &s); err != nil {
				return nil, errors.WithStack(err)
			}

			vals[key] = []byte(s)
		}
	}

	if !fixed {
		return nil, nil
	}

	encoded, err := jsoniter.Marshal(vals)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return encoded, nil
}
