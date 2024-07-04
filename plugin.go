package memcached

import (
	"github.com/roadrunner-server/api/v4/plugins/v1/kv"
	"github.com/roadrunner-server/endure/v2/dep"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/memcached/v5/memcachedkv"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
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

type Logger interface {
	NamedLogger(name string) *zap.Logger
}

// Tracer is a plugin (OTEL) interface that provides tracer
type Tracer interface {
	Tracer() *sdktrace.TracerProvider
}

type Plugin struct {
	// logger
	log *zap.Logger
	// config plugin
	cfgPlugin Configurer
	tracer    *sdktrace.TracerProvider
}

func (p *Plugin) Init(log Logger, cfg Configurer) error {
	if !cfg.Has(RootPluginName) {
		return errors.E(errors.Disabled)
	}

	p.cfgPlugin = cfg
	p.log = log.NamedLogger(PluginName)
	p.tracer = sdktrace.NewTracerProvider()
	return nil
}

// Name returns plugin user-friendly name
func (p *Plugin) Name() string {
	return PluginName
}

func (p *Plugin) Collects() []*dep.In {
	return []*dep.In{
		dep.Fits(func(pp any) {
			p.tracer = pp.(Tracer).Tracer()
		}, (*Tracer)(nil)),
	}
}

func (p *Plugin) KvFromConfig(key string) (kv.Storage, error) {
	const op = errors.Op("memcachedkv_plugin_provide")
	st, err := memcachedkv.NewMemcachedDriver(p.log, key, p.cfgPlugin, p.tracer)
	if err != nil {
		return nil, errors.E(op, err)
	}

	return st, nil
}
