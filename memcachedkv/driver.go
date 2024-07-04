package memcachedkv

import (
	stderr "errors"
	"strings"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/roadrunner-server/api/v4/plugins/v1/kv"
	"github.com/roadrunner-server/errors"
	"go.opentelemetry.io/contrib/instrumentation/github.com/bradfitz/gomemcache/memcache/otelmemcache"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if a config section exists.
	Has(name string) bool
}

type Driver struct {
	client *otelmemcache.Client
	log    *zap.Logger
	cfg    *Config
	tracer *sdktrace.TracerProvider
}

// NewMemcachedDriver returns a memcache client using the provided server(s)
// with equal weight. If a server is listed multiple times,
// it gets a proportional amount of weight.
func NewMemcachedDriver(log *zap.Logger, key string, cfgPlugin Configurer, tracer *sdktrace.TracerProvider) (*Driver, error) {
	const op = errors.Op("new_memcached_driver")

	s := &Driver{
		log:    log,
		tracer: tracer,
	}

	err := cfgPlugin.UnmarshalKey(key, &s.cfg)
	if err != nil {
		return nil, errors.E(op, err)
	}

	if s.cfg == nil {
		return nil, errors.E(op, errors.Errorf("config not found by provided key: %s", key))
	}

	s.cfg.InitDefaults()

	client := memcache.New(s.cfg.Addr...)
	s.client = otelmemcache.NewClientWithTracing(client, otelmemcache.WithTracerProvider(tracer))

	return s, nil
}

// Has checked the key for existence
func (d *Driver) Has(keys ...string) (map[string]bool, error) {
	const op = errors.Op("memcached_plugin_has")
	if keys == nil {
		return nil, errors.E(op, errors.NoKeys)
	}
	m := make(map[string]bool, len(keys))
	for i := range keys {
		keyTrimmed := strings.TrimSpace(keys[i])
		if keyTrimmed == "" {
			return nil, errors.E(op, errors.EmptyKey)
		}
		exist, err := d.client.Get(keys[i])

		if err != nil {
			// ErrCacheMiss means that a Get failed because the item wasn't present.
			if stderr.Is(err, memcache.ErrCacheMiss) {
				continue
			}
			return nil, errors.E(op, err)
		}
		if exist != nil {
			m[keys[i]] = true
		}
	}
	return m, nil
}

// Get gets the item for the given key. ErrCacheMiss is returned for a
// memcache cache miss. The key must be at most 250 bytes in length.
func (d *Driver) Get(key string) ([]byte, error) {
	const op = errors.Op("memcached_plugin_get")
	// to get cases like "  "
	keyTrimmed := strings.TrimSpace(key)
	if keyTrimmed == "" {
		return nil, errors.E(op, errors.EmptyKey)
	}
	data, err := d.client.Get(key)
	if err != nil {
		// ErrCacheMiss means that a Get failed because the item wasn't present.
		if stderr.Is(err, memcache.ErrCacheMiss) {
			return nil, nil
		}
		return nil, errors.E(op, err)
	}
	if data != nil {
		// return the value by the key
		return data.Value, nil
	}
	// data is nil by some reason and error also nil
	return nil, nil
}

// MGet return map with key -- string
// and map value as value -- []byte
func (d *Driver) MGet(keys ...string) (map[string][]byte, error) {
	const op = errors.Op("memcached_plugin_mget")
	if keys == nil {
		return nil, errors.E(op, errors.NoKeys)
	}

	// should not be empty keys
	for i := range keys {
		keyTrimmed := strings.TrimSpace(keys[i])
		if keyTrimmed == "" {
			return nil, errors.E(op, errors.EmptyKey)
		}
	}

	m := make(map[string][]byte, len(keys))
	for i := range keys {
		// Here also MultiGet
		data, err := d.client.Get(keys[i])
		if err != nil {
			// ErrCacheMiss means that a Get failed because the item wasn't present.
			if stderr.Is(err, memcache.ErrCacheMiss) {
				continue
			}
			return nil, errors.E(op, err)
		}
		if data != nil {
			m[keys[i]] = data.Value
		}
	}

	return m, nil
}

// Set sets the KV pairs. Keys should be 250 bytes maximum
// TTL:
// Expiration is the cache expiration time, in seconds: either a relative
// time from now (up to 1 month), or an absolute Unix epoch time.
// Zero means the Item has no expiration time.
func (d *Driver) Set(items ...kv.Item) error {
	const op = errors.Op("memcached_plugin_set")
	if items == nil {
		return errors.E(op, errors.NoKeys)
	}

	for i := range items {
		if items[i] == nil {
			return errors.E(op, errors.EmptyItem)
		}

		// pre-allocate item
		memcachedItem := &memcache.Item{
			Key: items[i].Key(),
			// unsafe convert
			Value: items[i].Value(),
			Flags: 0,
		}

		// add additional TTL in case of TTL isn't empty
		if items[i].Timeout() != "" {
			// verify the TTL
			t, err := time.Parse(time.RFC3339, items[i].Timeout())
			if err != nil {
				return err
			}
			memcachedItem.Expiration = int32(t.Unix())
		}

		err := d.client.Set(memcachedItem)
		if err != nil {
			return err
		}
	}

	return nil
}

// MExpire Expiration is the cache expiration time, in seconds: either a relative
// time from now (up to 1 month), or an absolute Unix epoch time.
// Zero means the Item has no expiration time.
func (d *Driver) MExpire(items ...kv.Item) error {
	const op = errors.Op("memcached_plugin_mexpire")
	for i := range items {
		if items[i] == nil {
			continue
		}
		if items[i].Timeout() == "" || strings.TrimSpace(items[i].Key()) == "" {
			return errors.E(op, errors.Str("should set timeout and at least one key"))
		}

		// verify provided TTL
		t, err := time.Parse(time.RFC3339, items[i].Timeout())
		if err != nil {
			return errors.E(op, err)
		}

		// Touch updates the expiry for the given key.
		// The second parameter is either
		// a Unix timestamp or, if seconds is less than 1 month, the number of seconds
		// into the future at which time the item will expire.
		// Zero means the item has
		// no expiration time.
		// ErrCacheMiss is returned if the key is not in the cache.
		// The key must be at most 250 bytes in length.
		err = d.client.Touch(items[i].Key(), int32(t.Unix()))
		if err != nil {
			return errors.E(op, err)
		}
	}
	return nil
}

// TTL return time in seconds (int32) for a given keys
func (d *Driver) TTL(_ ...string) (map[string]string, error) {
	const op = errors.Op("memcached_plugin_ttl")
	return nil, errors.E(op, errors.Str("not valid request for memcached, see https://github.com/memcached/memcached/issues/239"))
}

func (d *Driver) Delete(keys ...string) error {
	const op = errors.Op("memcached_plugin_delete")
	if keys == nil {
		return errors.E(op, errors.NoKeys)
	}

	// should not be empty keys
	for i := range keys {
		keyTrimmed := strings.TrimSpace(keys[i])
		if keyTrimmed == "" {
			return errors.E(op, errors.EmptyKey)
		}
	}

	for i := range keys {
		err := d.client.Delete(keys[i])
		// ErrCacheMiss means that a Get failed because the item wasn't present.
		if err != nil {
			// ErrCacheMiss means that a Get failed because the item wasn't present.
			if stderr.Is(err, memcache.ErrCacheMiss) {
				continue
			}
			return errors.E(op, err)
		}
	}
	return nil
}

func (d *Driver) Clear() error {
	err := d.client.DeleteAll()
	if err != nil {
		d.log.Error("Clear (delete_all) operation failed", zap.Error(err))
		return err
	}

	return nil
}

func (d *Driver) Stop() {
	// not implemented https://github.com/bradfitz/gomemcache/issues/51
}
