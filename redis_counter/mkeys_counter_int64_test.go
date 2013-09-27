package redis_counter

import "math"
import "github.com/alecthomas/log4go"
import "github.com/gnagel/dog_pool/dog_pool"
import "testing"
import "github.com/orfjackal/gospec/src/gospec"

func TestRedisMKeysCounterInt64Specs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(RedisMKeysCounterInt64Specs)
	gospec.MainGoTest(r, t)
}

func RedisMKeysCounterInt64Specs(c gospec.Context) {

	c.Specify("[RedisMKeysCounterInt64][Make] Makes new instance", func() {
		value, err := MakeRedisMKeysCounterInt64(nil, "")
		c.Expect(err.Error(), gospec.Equals, "Nil redis connection")
		c.Expect(value, gospec.Satisfies, nil == value)

		value, err = MakeRedisMKeysCounterInt64(&dog_pool.RedisConnection{}, "")
		c.Expect(err.Error(), gospec.Equals, "Empty redis key[0]")
		c.Expect(value, gospec.Satisfies, nil == value)

		value, err = MakeRedisMKeysCounterInt64(&dog_pool.RedisConnection{}, "Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, nil != value)
	})

	c.Specify("[RedisMKeysCounterInt64][String] Formats string", func() {
		value, _ := MakeRedisMKeysCounterInt64(&dog_pool.RedisConnection{}, "Bob", "Gary", "AAA", "Missing")

		// Order of Keys determines output order
		c.Expect(value.String(), gospec.Equals, "Bob = NaN, Gary = NaN, AAA = NaN, Missing = NaN")

		counter := int64(123)
		value.cache.m = map[string]*int64{
			"AAA":  &counter,
			"Bob":  &counter,
			"Gary": &counter,
		}

		// Order of Keys determines output order
		c.Expect(value.String(), gospec.Equals, "Bob = 123, Gary = 123, AAA = 123, Missing = NaN")
	})

	c.Specify("[RedisMKeysCounterInt64][MExists] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisMKeysCounterInt64(server.Connection(), "Bob", "George", "Alex", "Applause")

		// Valid number:
		for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
			server.Connection().Cmd("SET", key, "123")
		}

		oks, err := value.MExists()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(oks), gospec.Equals, len(value.KEYS))
		for _, ok := range oks {
			c.Expect(ok, gospec.Equals, true)
		}
		c.Expect(value.cache.Len(), gospec.Equals, 0)

		// Cache Miss
		server.Connection().Cmd("DEL", value.KEYS)
		oks, err = value.MExists()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(oks), gospec.Equals, len(value.KEYS))
		for _, ok := range oks {
			c.Expect(ok, gospec.Equals, false)
		}
		c.Expect(value.cache.Len(), gospec.Equals, 0)
	})

	c.Specify("[RedisMKeysCounterInt64][MInt64] Gets value from Redis", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisMKeysCounterInt64(server.Connection(), "Bob", "George", "Alex", "Applause")

		// Valid number:
		for i, key := range []string{"Bob", "George", "Alex", "Applause"} {
			server.Connection().Cmd("SET", key, 123*math.Pow10(i))
		}

		counters, err := value.MInt64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.KEYS))
		for i, key := range value.KEYS {
			counter := counters[i]
			c.Expect(counter, gospec.Equals, int64(123*math.Pow10(i)))

			ptr := value.LastValue(key)
			c.Expect(*ptr, gospec.Equals, int64(123*math.Pow10(i)))
		}

		// Cache Miss
		server.Connection().Cmd("DEL", value.KEYS)
		counters, err = value.MInt64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value.cache.Len(), gospec.Equals, 0)
		c.Expect(len(counters), gospec.Equals, len(value.KEYS))
		for _, counter := range counters {
			c.Expect(counter, gospec.Equals, int64(0))
		}

		// Parsing error:
		for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
			server.Connection().Cmd("SET", key, "Gary")
		}

		counters, err = value.MInt64()
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(len(counters), gospec.Equals, 0)
		c.Expect(value.cache.Len(), gospec.Equals, 0)
	})

	c.Specify("[RedisMKeysCounterInt64][MGet] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisMKeysCounterInt64(server.Connection(), "Bob", "George", "Alex", "Applause")

		// Valid number:
		for i, key := range []string{"Bob", "George", "Alex", "Applause"} {
			server.Connection().Cmd("SET", key, 123*math.Pow10(i))
		}

		counters, err := value.MGet()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.KEYS))
		for i, key := range value.KEYS {
			counter := counters[i]
			c.Expect(counter, gospec.Equals, int64(123*math.Pow10(i)))

			ptr := value.LastValue(key)
			c.Expect(*ptr, gospec.Equals, int64(123*math.Pow10(i)))
		}

		// Cache Miss
		server.Connection().Cmd("DEL", value.KEYS)
		counters, err = value.MGet()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value.cache.Len(), gospec.Equals, 0)
		c.Expect(len(counters), gospec.Equals, len(value.KEYS))
		for _, counter := range counters {
			c.Expect(counter, gospec.Equals, int64(0))
		}

		// Parsing error:
		for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
			server.Connection().Cmd("SET", key, "Gary")
		}

		counters, err = value.MGet()
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(len(counters), gospec.Equals, 0)
		c.Expect(value.cache.Len(), gospec.Equals, 0)
	})

	c.Specify("[RedisMKeysCounterInt64][MDelete] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisMKeysCounterInt64(server.Connection(), "Bob", "George", "Alex", "Applause")

		// Valid number:
		for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
			server.Connection().Cmd("SET", key, "123")
		}

		err := value.MDelete()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value.cache.Len(), gospec.Equals, 0)

		for _, key := range value.KEYS {
			ok, _ := server.Connection().Cmd("EXISTS", key).Int()
			c.Expect(ok, gospec.Equals, 0)
		}
	})

	c.Specify("[RedisMKeysCounterInt64][MSet] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisMKeysCounterInt64(server.Connection(), "Bob", "George", "Alex", "Applause")

		counters, err := value.MSet(123)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.KEYS))
		for i, counter := range counters {
			c.Expect(counter, gospec.Equals, int64(123))

			value := value.LastValue(value.KEYS[i])
			c.Expect(value, gospec.Satisfies, nil != value)
			c.Expect(*value, gospec.Equals, int64(123))
		}

		list, list_err := server.Connection().Cmd("MGET", value.KEYS).List()
		c.Expect(list_err, gospec.Equals, nil)
		c.Expect(len(list), gospec.Equals, len(value.KEYS))

		for _, list_value := range list {
			c.Expect(list_value, gospec.Equals, "123")
		}
	})

	c.Specify("[RedisMKeysCounterInt64][MAdd] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisMKeysCounterInt64(server.Connection(), "Bob", "George", "Alex", "Applause")

		// Valid number:
		for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
			server.Connection().Cmd("SET", key, "123")
		}

		counters, err := value.MAdd(555)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.KEYS))
		for i, counter := range counters {
			c.Expect(counter, gospec.Equals, int64(123+555))

			value := value.LastValue(value.KEYS[i])
			c.Expect(value, gospec.Satisfies, nil != value)
			c.Expect(*value, gospec.Equals, int64(123+555))
		}

		list, list_err := server.Connection().Cmd("MGET", value.KEYS).List()
		c.Expect(list_err, gospec.Equals, nil)
		c.Expect(len(list), gospec.Equals, len(value.KEYS))

		for _, list_value := range list {
			c.Expect(list_value, gospec.Equals, "678")
		}
	})

	c.Specify("[RedisMKeysCounterInt64][MSub] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisMKeysCounterInt64(server.Connection(), "Bob", "George", "Alex", "Applause")

		// Valid number:
		for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
			server.Connection().Cmd("SET", key, "123")
		}

		counters, err := value.MSub(555)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.KEYS))
		for i, counter := range counters {
			c.Expect(counter, gospec.Equals, int64(123-555))

			value := value.LastValue(value.KEYS[i])
			c.Expect(value, gospec.Satisfies, nil != value)
			c.Expect(*value, gospec.Equals, int64(123-555))
		}

		list, list_err := server.Connection().Cmd("MGET", value.KEYS).List()
		c.Expect(list_err, gospec.Equals, nil)
		c.Expect(len(list), gospec.Equals, len(value.KEYS))

		for _, list_value := range list {
			c.Expect(list_value, gospec.Equals, "-432")
		}
	})

	c.Specify("[RedisMKeysCounterInt64][MIncrement] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisMKeysCounterInt64(server.Connection(), "Bob", "George", "Alex", "Applause")

		// Valid number:
		for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
			server.Connection().Cmd("SET", key, "123")
		}

		counters, err := value.MIncrement()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.KEYS))
		for i, counter := range counters {
			c.Expect(counter, gospec.Equals, int64(123+1))

			value := value.LastValue(value.KEYS[i])
			c.Expect(value, gospec.Satisfies, nil != value)
			c.Expect(*value, gospec.Equals, int64(123+1))
		}

		list, list_err := server.Connection().Cmd("MGET", value.KEYS).List()
		c.Expect(list_err, gospec.Equals, nil)
		c.Expect(len(list), gospec.Equals, len(value.KEYS))

		for _, list_value := range list {
			c.Expect(list_value, gospec.Equals, "124")
		}
	})

	c.Specify("[RedisMKeysCounterInt64][MDecrement] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisMKeysCounterInt64(server.Connection(), "Bob", "George", "Alex", "Applause")

		// Valid number:
		for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
			server.Connection().Cmd("SET", key, "123")
		}

		counters, err := value.MDecrement()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.KEYS))
		for i, counter := range counters {
			c.Expect(counter, gospec.Equals, int64(123-1))

			value := value.LastValue(value.KEYS[i])
			c.Expect(value, gospec.Satisfies, nil != value)
			c.Expect(*value, gospec.Equals, int64(123-1))
		}

		list, list_err := server.Connection().Cmd("MGET", value.KEYS).List()
		c.Expect(list_err, gospec.Equals, nil)
		c.Expect(len(list), gospec.Equals, len(value.KEYS))

		for _, list_value := range list {
			c.Expect(list_value, gospec.Equals, "122")
		}
	})

}

func Benchmark_RedisMKeysCounterInt64_MMake(b *testing.B) {
	for i := 0; i < b.N; i++ {
		MakeRedisMKeysCounterInt64(&dog_pool.RedisConnection{}, "Bob")
	}
}

func Benchmark_RedisMKeysCounterInt64_MString(b *testing.B) {
	value, _ := MakeRedisMKeysCounterInt64(&dog_pool.RedisConnection{}, "Bob")
	for i := 0; i < b.N; i++ {
		value.String()
	}
}

func Benchmark_RedisMKeysCounterInt64_MExists(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterInt64(server.Connection(), "Bob", "George", "Alex", "Applause")

	// Valid number:
	for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
		server.Connection().Cmd("SET", key, "123")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MExists()
	}
}

func Benchmark_RedisMKeysCounterInt64_MInt64_ValidNumber(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterInt64(server.Connection(), "Bob", "George", "Alex", "Applause")

	// Valid number:
	for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
		server.Connection().Cmd("SET", key, "123")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MInt64()
	}
}

func Benchmark_RedisMKeysCounterInt64_MInt64_CacheMiss(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterInt64(server.Connection(), "Bob", "George", "Alex", "Applause")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MInt64()
	}
}

func Benchmark_RedisMKeysCounterInt64_MInt64_InvalidNumber(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterInt64(server.Connection(), "Bob", "George", "Alex", "Applause")

	// Valid number:
	for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
		server.Connection().Cmd("SET", key, "Gary")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MInt64()
	}
}

func Benchmark_RedisMKeysCounterInt64_MGet(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterInt64(server.Connection(), "Bob", "George", "Alex", "Applause")

	// Valid number:
	for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
		server.Connection().Cmd("SET", key, "123")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MGet()
	}
}

func Benchmark_RedisMKeysCounterInt64_MSet(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterInt64(server.Connection(), "Bob", "George", "Alex", "Applause")

	// Valid number:
	for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
		server.Connection().Cmd("SET", key, "000")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MSet(123)
	}
}

func Benchmark_RedisMKeysCounterInt64_MDelete(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterInt64(server.Connection(), "Bob", "George", "Alex", "Applause")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MDelete()
	}
}

func Benchmark_RedisMKeysCounterInt64_MAdd(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterInt64(server.Connection(), "Bob", "George", "Alex", "Applause")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MAdd(555)
	}
}

func Benchmark_RedisMKeysCounterInt64_MSub(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterInt64(server.Connection(), "Bob", "George", "Alex", "Applause")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MSub(555)
	}
}

func Benchmark_RedisMKeysCounterInt64_MIncrement(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterInt64(server.Connection(), "Bob", "George", "Alex", "Applause")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MIncrement()
	}
}

func Benchmark_RedisMKeysCounterInt64_MDecrement(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterInt64(server.Connection(), "Bob", "George", "Alex", "Applause")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MDecrement()
	}
}
