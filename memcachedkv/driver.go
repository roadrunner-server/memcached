package memcachedkv

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/roadrunner-server/api-plugins/v6/kv"
	rrerrors "github.com/roadrunner-server/errors"
	"go.opentelemetry.io/contrib/instrumentation/github.com/bradfitz/gomemcache/memcache/otelmemcache"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if a config section exists.
	Has(name string) bool
}

type Driver struct {
	client *otelmemcache.Client
	log    *slog.Logger
	cfg    *Config
	tracer *sdktrace.TracerProvider
}

// NewMemcachedDriver returns a memcache client using the provided server(s)
// with equal weight. If a server is listed multiple times,
// it gets a proportional amount of weight.
func NewMemcachedDriver(log *slog.Logger, key string, cfgPlugin Configurer, tracer *sdktrace.TracerProvider) (*Driver, error) {
	const op = rrerrors.Op("new_memcached_driver")

	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	s := &Driver{
		log:    log,
		tracer: tracer,
	}

	err := cfgPlugin.UnmarshalKey(key, &s.cfg)
	if err != nil {
		return nil, rrerrors.E(op, err)
	}

	if s.cfg == nil {
		return nil, rrerrors.E(op, rrerrors.Errorf("config not found by provided key: %s", key))
	}

	s.cfg.InitDefaults()

	client := memcache.New(s.cfg.Addr...)
	s.client = otelmemcache.NewClientWithTracing(client, otelmemcache.WithTracerProvider(tracer))

	return s, nil
}

// Has checked the key for existence
func (d *Driver) Has(_ context.Context, keys ...string) (map[string]bool, error) {
	const op = rrerrors.Op("memcached_plugin_has")
	if len(keys) == 0 {
		return nil, rrerrors.E(op, rrerrors.NoKeys)
	}
	m := make(map[string]bool, len(keys))
	for _, key := range keys {
		keyTrimmed := strings.TrimSpace(key)
		if keyTrimmed == "" {
			return nil, rrerrors.E(op, rrerrors.EmptyKey)
		}
		exist, err := d.client.Get(keyTrimmed)
		if err != nil {
			// ErrCacheMiss means that a Get failed because the item wasn't present.
			if errors.Is(err, memcache.ErrCacheMiss) {
				continue
			}
			return nil, rrerrors.E(op, err)
		}
		m[keyTrimmed] = exist != nil
	}
	return m, nil
}

// Get gets the item for the given key. ErrCacheMiss is returned for a
// memcache cache miss. The key must be at most 250 bytes in length.
func (d *Driver) Get(_ context.Context, key string) ([]byte, error) {
	const op = rrerrors.Op("memcached_plugin_get")
	if strings.TrimSpace(key) == "" {
		return nil, rrerrors.E(op, rrerrors.EmptyKey)
	}
	data, err := d.client.Get(key)
	if err != nil {
		// ErrCacheMiss means that a Get failed because the item wasn't present.
		if errors.Is(err, memcache.ErrCacheMiss) {
			return nil, nil
		}
		return nil, rrerrors.E(op, err)
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
func (d *Driver) MGet(_ context.Context, keys ...string) (map[string][]byte, error) {
	const op = rrerrors.Op("memcached_plugin_mget")
	if len(keys) == 0 {
		return nil, rrerrors.E(op, rrerrors.NoKeys)
	}

	for _, key := range keys {
		if strings.TrimSpace(key) == "" {
			return nil, rrerrors.E(op, rrerrors.EmptyKey)
		}
	}

	items, err := d.client.GetMulti(keys)
	if err != nil {
		return nil, rrerrors.E(op, err)
	}

	m := make(map[string][]byte, len(items))
	for k, item := range items {
		if item != nil {
			m[k] = item.Value
		}
	}

	return m, nil
}

// Set sets the KV pairs. Keys should be 250 bytes maximum
// TTL:
// Expiration is the cache expiration time, in seconds: either a relative
// time from now (up to 1 month), or an absolute Unix epoch time.
// Zero means the Item has no expiration time.
func (d *Driver) Set(_ context.Context, items ...kv.Item) error {
	const op = rrerrors.Op("memcached_plugin_set")
	if len(items) == 0 {
		return rrerrors.E(op, rrerrors.NoKeys)
	}

	for _, item := range items {
		if item == nil {
			return rrerrors.E(op, rrerrors.EmptyItem)
		}

		// pre-allocate item
		memcachedItem := &memcache.Item{
			Key:   item.Key(),
			Value: item.Value(),
		}

		// add additional TTL in case of TTL isn't empty
		if item.Timeout() != "" {
			// verify the TTL
			t, err := time.Parse(time.RFC3339, item.Timeout())
			if err != nil {
				return rrerrors.E(op, err)
			}
			memcachedItem.Expiration = int32(t.Unix()) //nolint:gosec
		}

		if err := d.client.Set(memcachedItem); err != nil {
			return rrerrors.E(op, err)
		}
	}

	return nil
}

// MExpire Expiration is the cache expiration time, in seconds: either a relative
// time from now (up to 1 month), or an absolute Unix epoch time.
// Zero means the Item has no expiration time.
func (d *Driver) MExpire(_ context.Context, items ...kv.Item) error {
	const op = rrerrors.Op("memcached_plugin_mexpire")
	for _, item := range items {
		if item == nil {
			continue
		}
		if item.Timeout() == "" || strings.TrimSpace(item.Key()) == "" {
			return rrerrors.E(op, rrerrors.Str("should set timeout and at least one key"))
		}

		// verify provided TTL
		t, err := time.Parse(time.RFC3339, item.Timeout())
		if err != nil {
			return rrerrors.E(op, err)
		}

		// Touch updates the expiry for the given key.
		// The second parameter is either
		// a Unix timestamp or, if seconds is less than 1 month, the number of seconds
		// into the future at which time the item will expire.
		// Zero means the item has
		// no expiration time.
		// ErrCacheMiss is returned if the key is not in the cache.
		// The key must be at most 250 bytes in length.
		err = d.client.Touch(item.Key(), int32(t.Unix())) //nolint:gosec
		if err != nil {
			return rrerrors.E(op, err)
		}
	}
	return nil
}

// TTL return time in seconds (int32) for a given keys
func (d *Driver) TTL(_ context.Context, _ ...string) (map[string]string, error) {
	const op = rrerrors.Op("memcached_plugin_ttl")
	return nil, rrerrors.E(op, rrerrors.Str("not valid request for memcached, see https://github.com/memcached/memcached/issues/239"))
}

func (d *Driver) Delete(_ context.Context, keys ...string) error {
	const op = rrerrors.Op("memcached_plugin_delete")
	if len(keys) == 0 {
		return rrerrors.E(op, rrerrors.NoKeys)
	}

	for _, key := range keys {
		keyTrimmed := strings.TrimSpace(key)
		if keyTrimmed == "" {
			return rrerrors.E(op, rrerrors.EmptyKey)
		}
		err := d.client.Delete(keyTrimmed)
		if err != nil {
			// ErrCacheMiss means that a Get failed because the item wasn't present.
			if errors.Is(err, memcache.ErrCacheMiss) {
				continue
			}
			return rrerrors.E(op, err)
		}
	}
	return nil
}

func (d *Driver) Clear(_ context.Context) error {
	const op = rrerrors.Op("memcached_plugin_clear")
	if err := d.client.DeleteAll(); err != nil {
		d.log.Error("Clear (delete_all) operation failed", "error", err)
		return rrerrors.E(op, err)
	}

	return nil
}

func (d *Driver) Stop(_ context.Context) {
	// not implemented https://github.com/bradfitz/gomemcache/issues/51
}
