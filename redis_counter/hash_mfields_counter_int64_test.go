package redis_counter

import "math"
import "github.com/alecthomas/log4go"
import "github.com/gnagel/dog_pool/dog_pool"
import "testing"
import "github.com/orfjackal/gospec/src/gospec"

func TestRedisHashMFieldsCounterInt64Specs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(RedisHashMFieldsCounterInt64Specs)
	gospec.MainGoTest(r, t)
}

func RedisHashMFieldsCounterInt64Specs(c gospec.Context) {

	c.Specify("[RedisHashMFieldsCounterInt64][Make] Makes new instance", func() {
		value, err := MakeRedisHashMFieldsCounterInt64(nil, "")
		c.Expect(err.Error(), gospec.Equals, "Nil redis connection")
		c.Expect(value, gospec.Satisfies, nil == value)

		value, err = MakeRedisHashMFieldsCounterInt64(&dog_pool.RedisConnection{}, "")
		c.Expect(err.Error(), gospec.Equals, "Empty redis key")
		c.Expect(value, gospec.Satisfies, nil == value)

		value, err = MakeRedisHashMFieldsCounterInt64(&dog_pool.RedisConnection{}, "Key", "")
		c.Expect(err.Error(), gospec.Equals, "Empty redis field[0]")
		c.Expect(value, gospec.Satisfies, nil == value)

		value, err = MakeRedisHashMFieldsCounterInt64(&dog_pool.RedisConnection{}, "Key", "Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.KEY, gospec.Equals, "Key")
		c.Expect(len(value.FIELDS), gospec.Equals, 1)
		c.Expect(value.FIELDS[0], gospec.Equals, "Bob")
	})

	c.Specify("[RedisHashMFieldsCounterInt64][String] Formats string", func() {
		value, _ := MakeRedisHashMFieldsCounterInt64(&dog_pool.RedisConnection{}, "Key", "Bob", "Gary", "AAA", "Missing")

		// Order of FIELDS determines output order
		c.Expect(value.String(), gospec.Equals, "Key[Bob = NaN, Gary = NaN, AAA = NaN, Missing = NaN]")

		counter := int64(123)
		value.Cache.Set("AAA", &counter)
		value.Cache.Set("Bob", &counter)
		value.Cache.Set("Gary", &counter)

		// Order of FIELDS determines output order
		c.Expect(value.String(), gospec.Equals, "Key[Bob = 123, Gary = 123, AAA = 123, Missing = NaN]")
	})

	c.Specify("[RedisHashMFieldsCounterInt64][MExists] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashMFieldsCounterInt64(server.Connection(), "Key", "Bob", "George", "Alex", "Applause")

		// Valid number:
		for _, field := range value.FIELDS {
			server.Connection().Cmd("HSET", value.KEY, field, "123")
		}

		oks, err := value.MExists()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(oks), gospec.Equals, len(value.FIELDS))
		for _, ok := range oks {
			c.Expect(ok, gospec.Equals, true)
		}
		c.Expect(value.Cache.Len(), gospec.Equals, 0)

		// Cache Miss
		server.Connection().Cmd("HDEL", value.KEY, value.FIELDS)
		oks, err = value.MExists()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(oks), gospec.Equals, len(value.FIELDS))
		for _, ok := range oks {
			c.Expect(ok, gospec.Equals, false)
		}
		c.Expect(value.Cache.Len(), gospec.Equals, 0)
	})

	c.Specify("[RedisHashMFieldsCounterInt64][MInt64] Gets value from Redis", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashMFieldsCounterInt64(server.Connection(), "Key", "Bob", "George", "Alex", "Applause")

		// Valid number:
		for i, field := range value.FIELDS {
			server.Connection().Cmd("HSET", value.KEY, field, 123*math.Pow10(i))
		}

		counters, err := value.MInt64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.FIELDS))
		for i, field := range value.FIELDS {
			counter := counters[i]
			c.Expect(counter, gospec.Equals, int64(123*math.Pow10(i)))

			ptr := value.Cache.Value(field)
			c.Expect(*ptr, gospec.Equals, int64(123*math.Pow10(i)))
		}

		// Cache Miss
		server.Connection().Cmd("HDEL", value.KEY, value.FIELDS)
		counters, err = value.MInt64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value.Cache.Len(), gospec.Equals, 0)
		c.Expect(len(counters), gospec.Equals, len(value.FIELDS))
		for _, counter := range counters {
			c.Expect(counter, gospec.Equals, int64(0))
		}

		// Parsing error:
		for _, field := range value.FIELDS {
			server.Connection().Cmd("HSET", value.KEY, field, "Gary")
		}

		counters, err = value.MInt64()
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(len(counters), gospec.Equals, 0)
		c.Expect(value.Cache.Len(), gospec.Equals, 0)
	})

	c.Specify("[RedisHashMFieldsCounterInt64][MGet] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashMFieldsCounterInt64(server.Connection(), "Key", "Bob", "George", "Alex", "Applause")

		// Valid number:
		for i, field := range value.FIELDS {
			server.Connection().Cmd("HSET", value.KEY, field, 123*math.Pow10(i))
		}

		counters, err := value.MGet()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.FIELDS))
		for i, field := range value.FIELDS {
			counter := counters[i]
			c.Expect(counter, gospec.Equals, int64(123*math.Pow10(i)))

			ptr := value.Cache.Value(field)
			c.Expect(*ptr, gospec.Equals, int64(123*math.Pow10(i)))
		}

		// Cache Miss
		server.Connection().Cmd("HDEL", value.KEY, value.FIELDS)
		counters, err = value.MGet()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value.Cache.Len(), gospec.Equals, 0)
		c.Expect(len(counters), gospec.Equals, len(value.FIELDS))
		for _, counter := range counters {
			c.Expect(counter, gospec.Equals, int64(0))
		}

		// Parsing error:
		for _, field := range value.FIELDS {
			server.Connection().Cmd("HSET", value.KEY, field, "Gary")
		}

		counters, err = value.MGet()
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(len(counters), gospec.Equals, 0)
		c.Expect(value.Cache.Len(), gospec.Equals, 0)
	})

	c.Specify("[RedisHashMFieldsCounterInt64][MDelete] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashMFieldsCounterInt64(server.Connection(), "Key", "Bob", "George", "Alex", "Applause")

		// Valid number:
		for _, field := range value.FIELDS {
			server.Connection().Cmd("HSET", value.KEY, field, "123")
		}

		err := value.MDelete()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value.Cache.Len(), gospec.Equals, 0)

		for _, field := range value.FIELDS {
			ok, _ := server.Connection().Cmd("HEXISTS", value.KEY, field).Int()
			c.Expect(ok, gospec.Equals, 0)
		}
	})

	c.Specify("[RedisHashMFieldsCounterInt64][MSet] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashMFieldsCounterInt64(server.Connection(), "Key", "Bob", "George", "Alex", "Applause")

		counters, err := value.MSet(123)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.FIELDS))
		for i, counter := range counters {
			c.Expect(counter, gospec.Equals, int64(123))

			value := value.Cache.Value(value.FIELDS[i])
			c.Expect(value, gospec.Satisfies, nil != value)
			c.Expect(*value, gospec.Equals, int64(123))
		}

		list, list_err := server.Connection().Cmd("HMGET", value.KEY, value.FIELDS).List()
		c.Expect(list_err, gospec.Equals, nil)
		c.Expect(len(list), gospec.Equals, len(value.FIELDS))

		for _, list_value := range list {
			c.Expect(list_value, gospec.Equals, "123")
		}
	})

	c.Specify("[RedisHashMFieldsCounterInt64][MAdd] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashMFieldsCounterInt64(server.Connection(), "Key", "Bob", "George", "Alex", "Applause")

		// Valid number:
		for _, field := range value.FIELDS {
			server.Connection().Cmd("HSET", value.KEY, field, "123")
		}

		counters, err := value.MAdd(555)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.FIELDS))
		for i, counter := range counters {
			c.Expect(counter, gospec.Equals, int64(123+555))

			value := value.Cache.Value(value.FIELDS[i])
			c.Expect(value, gospec.Satisfies, nil != value)
			c.Expect(*value, gospec.Equals, int64(123+555))
		}

		list, list_err := server.Connection().Cmd("HMGET", value.KEY, value.FIELDS).List()
		c.Expect(list_err, gospec.Equals, nil)
		c.Expect(len(list), gospec.Equals, len(value.FIELDS))

		for _, list_value := range list {
			c.Expect(list_value, gospec.Equals, "678")
		}
	})

	c.Specify("[RedisHashMFieldsCounterInt64][MSub] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashMFieldsCounterInt64(server.Connection(), "Key", "Bob", "George", "Alex", "Applause")

		// Valid number:
		for _, field := range value.FIELDS {
			server.Connection().Cmd("HSET", value.KEY, field, "123")
		}

		counters, err := value.MSub(555)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.FIELDS))
		for i, counter := range counters {
			c.Expect(counter, gospec.Equals, int64(123-555))

			value := value.Cache.Value(value.FIELDS[i])
			c.Expect(value, gospec.Satisfies, nil != value)
			c.Expect(*value, gospec.Equals, int64(123-555))
		}

		list, list_err := server.Connection().Cmd("HMGET", value.KEY, value.FIELDS).List()
		c.Expect(list_err, gospec.Equals, nil)
		c.Expect(len(list), gospec.Equals, len(value.FIELDS))

		for _, list_value := range list {
			c.Expect(list_value, gospec.Equals, "-432")
		}
	})

	c.Specify("[RedisHashMFieldsCounterInt64][MIncrement] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashMFieldsCounterInt64(server.Connection(), "Key", "Bob", "George", "Alex", "Applause")

		// Valid number:
		for _, field := range value.FIELDS {
			server.Connection().Cmd("HSET", value.KEY, field, "123")
		}

		counters, err := value.MIncrement()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.FIELDS))
		for i, counter := range counters {
			c.Expect(counter, gospec.Equals, int64(123+1))

			value := value.Cache.Value(value.FIELDS[i])
			c.Expect(value, gospec.Satisfies, nil != value)
			c.Expect(*value, gospec.Equals, int64(123+1))
		}

		list, list_err := server.Connection().Cmd("HMGET", value.KEY, value.FIELDS).List()
		c.Expect(list_err, gospec.Equals, nil)
		c.Expect(len(list), gospec.Equals, len(value.FIELDS))

		for _, list_value := range list {
			c.Expect(list_value, gospec.Equals, "124")
		}
	})

	c.Specify("[RedisHashMFieldsCounterInt64][MDecrement] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashMFieldsCounterInt64(server.Connection(), "Key", "Bob", "George", "Alex", "Applause")

		// Valid number:
		for _, field := range value.FIELDS {
			server.Connection().Cmd("HSET", value.KEY, field, "123")
		}

		counters, err := value.MDecrement()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(counters), gospec.Equals, len(value.FIELDS))
		for i, counter := range counters {
			c.Expect(counter, gospec.Equals, int64(123-1))

			value := value.Cache.Value(value.FIELDS[i])
			c.Expect(value, gospec.Satisfies, nil != value)
			c.Expect(*value, gospec.Equals, int64(123-1))
		}

		list, list_err := server.Connection().Cmd("HMGET", value.KEY, value.FIELDS).List()
		c.Expect(list_err, gospec.Equals, nil)
		c.Expect(len(list), gospec.Equals, len(value.FIELDS))

		for _, list_value := range list {
			c.Expect(list_value, gospec.Equals, "122")
		}
	})

}

func Benchmark_RedisHashMFieldsCounterInt64_MMake(b *testing.B) {
	for i := 0; i < b.N; i++ {
		MakeRedisHashMFieldsCounterInt64(&dog_pool.RedisConnection{}, "Key", "Bob", "George", "Alex", "Applause")
	}
}

func Benchmark_RedisHashMFieldsCounterInt64_MString(b *testing.B) {
	value, _ := MakeRedisHashMFieldsCounterInt64(&dog_pool.RedisConnection{}, "Key", "Bob", "George", "Alex", "Applause")
	for i := 0; i < b.N; i++ {
		value.String()
	}
}

func Benchmark_RedisHashMFieldsCounterInt64_MExists(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashMFieldsCounterInt64(server.Connection(), "Key", "Bob", "George", "Alex", "Applause")

	// Valid number:
	for _, field := range value.FIELDS {
		server.Connection().Cmd("HSET", value.KEY, field, "123")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MExists()
	}
}

func Benchmark_RedisHashMFieldsCounterInt64_MInt64_ValidNumber(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashMFieldsCounterInt64(server.Connection(), "Key", "Bob", "George", "Alex", "Applause")

	// Valid number:
	for _, field := range value.FIELDS {
		server.Connection().Cmd("HSET", value.KEY, field, "123")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MInt64()
	}
}

func Benchmark_RedisHashMFieldsCounterInt64_MInt64_CacheMiss(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashMFieldsCounterInt64(server.Connection(), "Key", "Bob", "George", "Alex", "Applause")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MInt64()
	}
}

func Benchmark_RedisHashMFieldsCounterInt64_MInt64_InvalidNumber(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashMFieldsCounterInt64(server.Connection(), "Key", "Bob", "George", "Alex", "Applause")

	// Valid number:
	for _, field := range value.FIELDS {
		server.Connection().Cmd("HSET", value.KEY, field, "Gary")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MInt64()
	}
}

func Benchmark_RedisHashMFieldsCounterInt64_MGet(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashMFieldsCounterInt64(server.Connection(), "Key", "Bob", "George", "Alex", "Applause")

	// Valid number:
	for _, field := range value.FIELDS {
		server.Connection().Cmd("HSET", value.KEY, field, "123")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MGet()
	}
}

func Benchmark_RedisHashMFieldsCounterInt64_MSet(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashMFieldsCounterInt64(server.Connection(), "Key", "Bob", "George", "Alex", "Applause")

	// Valid number:
	for _, field := range value.FIELDS {
		server.Connection().Cmd("HSET", value.KEY, field, "000")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MSet(123)
	}
}

func Benchmark_RedisHashMFieldsCounterInt64_MDelete(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashMFieldsCounterInt64(server.Connection(), "Key", "Bob", "George", "Alex", "Applause")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MDelete()
	}
}

func Benchmark_RedisHashMFieldsCounterInt64_MAdd(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashMFieldsCounterInt64(server.Connection(), "Key", "Bob", "George", "Alex", "Applause")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MAdd(555)
	}
}

func Benchmark_RedisHashMFieldsCounterInt64_MSub(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashMFieldsCounterInt64(server.Connection(), "Key", "Bob", "George", "Alex", "Applause")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MSub(555)
	}
}

func Benchmark_RedisHashMFieldsCounterInt64_MIncrement(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashMFieldsCounterInt64(server.Connection(), "Key", "Bob", "George", "Alex", "Applause")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MIncrement()
	}
}

func Benchmark_RedisHashMFieldsCounterInt64_MDecrement(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashMFieldsCounterInt64(server.Connection(), "Key", "Bob", "George", "Alex", "Applause")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.MDecrement()
	}
}
