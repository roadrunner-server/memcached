package memcachedkv

type Config struct {
	// Addr is url for memcached, 11211 port is used by default
	Addr []string
}

func (s *Config) InitDefaults() {
	if len(s.Addr) == 0 {
		s.Addr = []string{"127.0.0.1:11211"} // default url for memcached
	}
}
