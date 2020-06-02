package entity

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
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
	Compress   bool
	// 某些由其它地方构造的缓存，其中存在字段内容进入缓存前先被json encode过
	// 这些字段缓存结果需要被decode两次才能使用
	RecursiveDecode []string
}

func loadCache(ent Cacheable) (bool, error) {
	opt, err := getCacheOption(ent)
	if err != nil {
		return false, err
	}

	data, err := opt.Cacher.Get(opt.Key)
	if err != nil {
		return false, errors.WithStack(err)
	} else if len(data) == 0 {
		return false, nil
	}

	if opt.Compress {
		zr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return false, fmt.Errorf("uncompress cache data, %w", err)
		}
		defer zr.Close()

		v, err := ioutil.ReadAll(zr)
		if err != nil {
			return false, fmt.Errorf("uncompress cache data, %w", err)
		}
		data = v
	}

	if len(opt.RecursiveDecode) > 0 {
		fixed, err := recursiveDecode(data, opt.RecursiveDecode)
		if err != nil {
			return false, errors.WithMessage(err, "auto decode cache")
		}

		if fixed != nil {
			data = fixed
		}
	}

	if err := jsoniter.Unmarshal(data, ent); err != nil {
		return false, errors.WithStack(err)
	}

	return true, nil
}

// SaveCache 保存entity缓存
func SaveCache(ent Cacheable) error {
	data, err := jsoniter.Marshal(ent)
	if err != nil {
		return errors.WithStack(err)
	}

	opt, err := getCacheOption(ent)
	if err != nil {
		return err
	}

	if opt.Compress {
		var zdata bytes.Buffer
		zw := gzip.NewWriter(&zdata)
		if _, err := zw.Write(data); err != nil {
			return fmt.Errorf("compress cache data, %w", err)
		}
		if err := zw.Close(); err != nil {
			return fmt.Errorf("comporess cache data, %w", err)
		}
		data = zdata.Bytes()
	}

	return errors.Wrap(opt.Cacher.Put(opt.Key, data, opt.Expiration), "save entity cache")
}

// DeleteCache 删除entity缓存
func DeleteCache(ent Cacheable) error {
	opt, err := getCacheOption(ent)
	if err != nil {
		return err
	}

	return errors.Wrap(opt.Cacher.Delete(opt.Key), "delete entity cache")
}

func getCacheOption(ent Cacheable) (*CacheOption, error) {
	opt := ent.CacheOption()

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

func recursiveDecode(data []byte, keys []string) ([]byte, error) {
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
