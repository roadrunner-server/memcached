package kv

import (
	"testing"
	"time"

	kvProto "github.com/roadrunner-server/api-go/v6/kv/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	// shortTTL is the lifetime given to keys that are expected to expire during
	// a test. memcached resolves TTLs at one-second granularity, so this cannot
	// go below a second.
	shortTTL = time.Second * 2
	// expiryWait bounds the poll for an expiry, generously enough to survive a
	// loaded CI runner.
	expiryWait = time.Second * 30
	expiryTick = time.Millisecond * 250
)

// TestKeyExpiresAfterTTL sets one key with a TTL and one without, then polls
// until the first is gone. Polling rather than sleeping keeps the test at the
// real expiry time instead of a fixed worst-case wait.
func TestKeyExpiresAfterTTL(t *testing.T) {
	client := bootKV(t)

	req := &kvProto.KvRequest{
		Storage: storage,
		Items: []*kvProto.KvItem{
			{Key: "permanent", Value: []byte("v")},
			{Key: "ephemeral", Value: []byte("v"), Ttl: durationpb.New(shortTTL)},
		},
	}
	require.NoError(t, client.Call("kv.Set", req, &kvProto.KvResponse{}))

	require.Equal(t, 2, has(t, client, "permanent", "ephemeral"), "both keys should be present right after Set")

	require.Eventually(t, func() bool {
		return has(t, client, "ephemeral") == 0
	}, expiryWait, expiryTick, "the key with a TTL never expired")

	require.Equal(t, 1, has(t, client, "permanent"), "the key without a TTL must survive")
}

// TestMExpireAppliesTTLToExistingKeys stores keys with no TTL, then adds one
// through MExpire and waits for them to go.
func TestMExpireAppliesTTLToExistingKeys(t *testing.T) {
	client := bootKV(t)

	require.NoError(t, client.Call("kv.Set", items(map[string]string{"a": "aa", "b": "bb"}), &kvProto.KvResponse{}))

	expire := &kvProto.KvRequest{
		Storage: storage,
		Items: []*kvProto.KvItem{
			{Key: "a", Ttl: durationpb.New(shortTTL)},
			{Key: "b", Ttl: durationpb.New(shortTTL)},
		},
	}
	require.NoError(t, client.Call("kv.MExpire", expire, &kvProto.KvResponse{}))

	require.Eventually(t, func() bool {
		return has(t, client, "a", "b") == 0
	}, expiryWait, expiryTick, "keys did not expire after MExpire")
}

// TestTTLIsNotSupported pins the driver's limitation: memcached exposes no way
// to read a key's remaining lifetime, so the call must fail rather than answer
// with a wrong value.
func TestTTLIsNotSupported(t *testing.T) {
	client := bootKV(t)

	require.NoError(t, client.Call("kv.Set", items(map[string]string{"a": "aa"}), &kvProto.KvResponse{}))

	err := client.Call("kv.TTL", keys("a"), &kvProto.KvResponse{})

	require.Error(t, err)
}
