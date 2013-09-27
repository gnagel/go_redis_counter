package redis_counter

import "math"
import "strconv"
import "github.com/alecthomas/log4go"
import "github.com/gnagel/dog_pool/dog_pool"
import "testing"
import "github.com/orfjackal/gospec/src/gospec"

func TestRedisMKeysCounterFloat64Specs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(RedisMKeysCounterFloat64Specs)
	gospec.MainGoTest(r, t)
}

func RedisMKeysCounterFloat64Specs(c gospec.Context) {

	c.Specify("[RedisMKeysCounterFloat64][Make] Makes new instance", func() {
		value, err := MakeRedisMKeysCounterFloat64(nil, "")
		c.Expect(err.Error(), gospec.Equals, "Nil redis connection")
		c.Expect(value, gospec.Satisfies, nil == value)

		value, err = MakeRedisMKeysCounterFloat64(&dog_pool.RedisConnection{}, "")
		c.Expect(err.Error(), gospec.Equals, "Empty redis key[0]")
		c.Expect(value, gospec.Satisfies, nil == value)

		value, err = MakeRedisMKeysCounterFloat64(&dog_pool.RedisConnection{}, "Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, nil != value)
	})

	c.Specify("[RedisMKeysCounterFloat64][String] Formats string", func() {
		value, _ := MakeRedisMKeysCounterFloat64(&dog_pool.RedisConnection{}, "Bob", "Gary", "AAA", "Missing")

		// Order of Keys determines output order
		c.Expect(value.String(), gospec.Equals, "Bob = NaN, Gary = NaN, AAA = NaN, Missing = NaN")

		counter := float64(123.456)
		value.cache.m = map[string]*float64{
			"AAA":  &counter,
			"Bob":  &counter,
			"Gary": &counter,
		}

		// Order of Keys determines output order
		c.Expect(value.String(), gospec.Equals, "Bob = 123.456000, Gary = 123.456000, AAA = 123.456000, Missing = NaN")
	})

	c.Specify("[RedisMKeysCounterFloat64][MExists] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisMKeysCounterFloat64(server.Connection(), "Bob", "George", "Alex", "Applause")

		// Valid number:
		for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
			server.Connection().Cmd("SET", key, "123.456")
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

	c.Specify("[RedisMKeysCounterFloat64][MFloat64] Gets value from Redis", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisMKeysCounterFloat64(server.Connection(), "Bob", "George", "Alex", "Applause")

		// Valid number:
		for i, key := range []string{"Bob", "George", "Alex", "Applause"} {
			server.Connection().Cmd("SET", key, 123.456*math.Pow10(i))
		}

		counters, err := value.MFloat64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.KEYS))
		for i, key := range value.KEYS {
			counter := counters[i]
			c.Expect(counter, gospec.Equals, float64(123.456*math.Pow10(i)))

			ptr := value.LastValue(key)
			c.Expect(*ptr, gospec.Equals, float64(123.456*math.Pow10(i)))
		}

		// Cache Miss
		server.Connection().Cmd("DEL", value.KEYS)
		counters, err = value.MFloat64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value.cache.Len(), gospec.Equals, 0)
		c.Expect(len(counters), gospec.Equals, len(value.KEYS))
		for _, counter := range counters {
			c.Expect(counter, gospec.Equals, float64(0))
		}

		// Parsing error:
		for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
			server.Connection().Cmd("SET", key, "Gary")
		}

		counters, err = value.MFloat64()
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(len(counters), gospec.Equals, 0)
		c.Expect(value.cache.Len(), gospec.Equals, 0)
	})

	c.Specify("[RedisMKeysCounterFloat64][MGet] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisMKeysCounterFloat64(server.Connection(), "Bob", "George", "Alex", "Applause")

		// Valid number:
		for i, key := range []string{"Bob", "George", "Alex", "Applause"} {
			server.Connection().Cmd("SET", key, 123.456*math.Pow10(i))
		}

		counters, err := value.MGet()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.KEYS))
		for i, key := range value.KEYS {
			counter := counters[i]
			c.Expect(counter, gospec.Equals, float64(123.456*math.Pow10(i)))

			ptr := value.LastValue(key)
			c.Expect(*ptr, gospec.Equals, float64(123.456*math.Pow10(i)))
		}

		// Cache Miss
		server.Connection().Cmd("DEL", value.KEYS)
		counters, err = value.MGet()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value.cache.Len(), gospec.Equals, 0)
		c.Expect(len(counters), gospec.Equals, len(value.KEYS))
		for _, counter := range counters {
			c.Expect(counter, gospec.Equals, float64(0))
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

	c.Specify("[RedisMKeysCounterFloat64][MDelete] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisMKeysCounterFloat64(server.Connection(), "Bob", "George", "Alex", "Applause")

		// Valid number:
		for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
			server.Connection().Cmd("SET", key, "123.456")
		}

		err := value.MDelete()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value.cache.Len(), gospec.Equals, 0)

		for _, key := range value.KEYS {
			ok, _ := server.Connection().Cmd("EXISTS", key).Int()
			c.Expect(ok, gospec.Equals, 0)
		}
	})

	c.Specify("[RedisMKeysCounterFloat64][MSet] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisMKeysCounterFloat64(server.Connection(), "Bob", "George", "Alex", "Applause")

		counters, err := value.MSet(123.456)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.KEYS))
		for i, counter := range counters {
			c.Expect(counter, gospec.Equals, float64(123.456))

			value := value.LastValue(value.KEYS[i])
			c.Expect(value, gospec.Satisfies, nil != value)
			c.Expect(*value, gospec.Equals, float64(123.456))
		}

		list, list_err := server.Connection().Cmd("MGET", value.KEYS).List()
		c.Expect(list_err, gospec.Equals, nil)
		c.Expect(len(list), gospec.Equals, len(value.KEYS))

		for _, list_value := range list {
			f, _ := strconv.ParseFloat(list_value, 64)
			c.Expect(list_value, gospec.Satisfies, f == 123.456)
		}
	})

	c.Specify("[RedisMKeysCounterFloat64][MAdd] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisMKeysCounterFloat64(server.Connection(), "Bob", "George", "Alex", "Applause")

		// Valid number:
		for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
			server.Connection().Cmd("SET", key, "123.456")
		}

		counters, err := value.MAdd(555)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.KEYS))
		for i, counter := range counters {
			c.Expect(counter, gospec.Equals, float64(123.456+555))

			value := value.LastValue(value.KEYS[i])
			c.Expect(value, gospec.Satisfies, nil != value)
			c.Expect(*value, gospec.Equals, float64(123.456+555))
		}

		list, list_err := server.Connection().Cmd("MGET", value.KEYS).List()
		c.Expect(list_err, gospec.Equals, nil)
		c.Expect(len(list), gospec.Equals, len(value.KEYS))

		for _, list_value := range list {
			c.Expect(list_value, gospec.Equals, "678.45600000000000002")
		}
	})

	c.Specify("[RedisMKeysCounterFloat64][MSub] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisMKeysCounterFloat64(server.Connection(), "Bob", "George", "Alex", "Applause")

		// Valid number:
		for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
			server.Connection().Cmd("SET", key, "123.456")
		}

		counters, err := value.MSub(555)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.KEYS))
		for i, counter := range counters {
			c.Expect(counter, gospec.Equals, float64(123.456-555))

			value := value.LastValue(value.KEYS[i])
			c.Expect(value, gospec.Satisfies, nil != value)
			c.Expect(*value, gospec.Equals, float64(123.456-555))
		}

		list, list_err := server.Connection().Cmd("MGET", value.KEYS).List()
		c.Expect(list_err, gospec.Equals, nil)
		c.Expect(len(list), gospec.Equals, len(value.KEYS))

		for _, list_value := range list {
			c.Expect(list_value, gospec.Equals, "-431.54399999999999998")
		}
	})

	c.Specify("[RedisMKeysCounterFloat64][MIncrement] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisMKeysCounterFloat64(server.Connection(), "Bob", "George", "Alex", "Applause")

		// Valid number:
		for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
			server.Connection().Cmd("SET", key, "123.456")
		}

		counters, err := value.MIncrement()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.KEYS))
		for i, counter := range counters {
			c.Expect(counter, gospec.Equals, float64(123.456+1))

			value := value.LastValue(value.KEYS[i])
			c.Expect(value, gospec.Satisfies, nil != value)
			c.Expect(*value, gospec.Equals, float64(123.456+1))
		}

		list, list_err := server.Connection().Cmd("MGET", value.KEYS).List()
		c.Expect(list_err, gospec.Equals, nil)
		c.Expect(len(list), gospec.Equals, len(value.KEYS))

		for _, list_value := range list {
			c.Expect(list_value, gospec.Equals, "124.456")
		}
	})

	c.Specify("[RedisMKeysCounterFloat64][MDecrement] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisMKeysCounterFloat64(server.Connection(), "Bob", "George", "Alex", "Applause")

		// Valid number:
		for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
			server.Connection().Cmd("SET", key, "123.456")
		}

		counters, err := value.MDecrement()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.KEYS))
		for i, counter := range counters {
			c.Expect(counter, gospec.Equals, float64(123.456-1))

			value := value.LastValue(value.KEYS[i])
			c.Expect(value, gospec.Satisfies, nil != value)
			c.Expect(*value, gospec.Equals, float64(123.456-1))
		}

		list, list_err := server.Connection().Cmd("MGET", value.KEYS).List()
		c.Expect(list_err, gospec.Equals, nil)
		c.Expect(len(list), gospec.Equals, len(value.KEYS))

		for _, list_value := range list {
			c.Expect(list_value, gospec.Equals, "122.456")
		}
	})

}

func Benchmark_RedisMKeysCounterFloat64_MMake(b *testing.B) {
	for i := 0; i < b.N; i++ {
		MakeRedisMKeysCounterFloat64(&dog_pool.RedisConnection{}, "Bob")
	}
}

func Benchmark_RedisMKeysCounterFloat64_MString(b *testing.B) {
	value, _ := MakeRedisMKeysCounterFloat64(&dog_pool.RedisConnection{}, "Bob")
	for i := 0; i < b.N; i++ {
		value.String()
	}
}

func Benchmark_RedisMKeysCounterFloat64_MExists(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterFloat64(server.Connection(), "Bob", "George", "Alex", "Applause")

	// Valid number:
	for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
		server.Connection().Cmd("SET", key, "123.456")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MExists()
	}
}

func Benchmark_RedisMKeysCounterFloat64_MFloat64_ValidNumber(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterFloat64(server.Connection(), "Bob", "George", "Alex", "Applause")

	// Valid number:
	for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
		server.Connection().Cmd("SET", key, "123.456")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MFloat64()
	}
}

func Benchmark_RedisMKeysCounterFloat64_MFloat64_CacheMiss(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterFloat64(server.Connection(), "Bob", "George", "Alex", "Applause")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MFloat64()
	}
}

func Benchmark_RedisMKeysCounterFloat64_MFloat64_InvalidNumber(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterFloat64(server.Connection(), "Bob", "George", "Alex", "Applause")

	// Valid number:
	for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
		server.Connection().Cmd("SET", key, "Gary")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MFloat64()
	}
}

func Benchmark_RedisMKeysCounterFloat64_MGet(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterFloat64(server.Connection(), "Bob", "George", "Alex", "Applause")

	// Valid number:
	for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
		server.Connection().Cmd("SET", key, "123.456")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MGet()
	}
}

func Benchmark_RedisMKeysCounterFloat64_MSet(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterFloat64(server.Connection(), "Bob", "George", "Alex", "Applause")

	// Valid number:
	for _, key := range []string{"Bob", "George", "Alex", "Applause"} {
		server.Connection().Cmd("SET", key, "000")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MSet(123.456)
	}
}

func Benchmark_RedisMKeysCounterFloat64_MDelete(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterFloat64(server.Connection(), "Bob", "George", "Alex", "Applause")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MDelete()
	}
}

func Benchmark_RedisMKeysCounterFloat64_MAdd(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterFloat64(server.Connection(), "Bob", "George", "Alex", "Applause")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MAdd(555)
	}
}

func Benchmark_RedisMKeysCounterFloat64_MSub(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterFloat64(server.Connection(), "Bob", "George", "Alex", "Applause")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MSub(555)
	}
}

func Benchmark_RedisMKeysCounterFloat64_MIncrement(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterFloat64(server.Connection(), "Bob", "George", "Alex", "Applause")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MIncrement()
	}
}

func Benchmark_RedisMKeysCounterFloat64_MDecrement(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisMKeysCounterFloat64(server.Connection(), "Bob", "George", "Alex", "Applause")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MDecrement()
	}
}
