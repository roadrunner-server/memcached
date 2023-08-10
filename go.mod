module github.com/roadrunner-server/memcached/v4

go 1.21

toolchain go1.21.0

require (
	github.com/bradfitz/gomemcache v0.0.0-20230611145640-acc696258285
	github.com/roadrunner-server/api/v4 v4.6.2
	github.com/roadrunner-server/errors v1.3.0
	go.uber.org/zap v1.25.0
)

require go.uber.org/multierr v1.11.0 // indirect
