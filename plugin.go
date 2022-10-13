package memcached

import (
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/memcached/v3/memcachedkv"
	"github.com/roadrunner-server/sdk/v3/plugins/kv"
	"go.uber.org/zap"
)

const (
	PluginName     string = "memcached"
	RootPluginName string = "kv"
)

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if config section exists.
	Has(name string) bool
}

type Plugin struct {
	// config plugin
	cfgPlugin Configurer
	// logger
	log *zap.Logger
}

func (p *Plugin) Init(log *zap.Logger, cfg Configurer) error {
	if !cfg.Has(RootPluginName) {
		return errors.E(errors.Disabled)
	}

	p.cfgPlugin = cfg
	p.log = new(zap.Logger)
	*p.log = *log
	return nil
}

// Name returns plugin user-friendly name
func (p *Plugin) Name() string {
	return PluginName
}

func (p *Plugin) KvFromConfig(key string) (kv.Storage, error) {
	const op = errors.Op("boltdb_plugin_provide")
	st, err := memcachedkv.NewMemcachedDriver(p.log, key, p.cfgPlugin)
	if err != nil {
		return nil, errors.E(op, err)
	}

	return st, nil
}
