package redis_counter

import "github.com/alecthomas/log4go"
import "github.com/gnagel/dog_pool/dog_pool"
import "testing"
import "github.com/orfjackal/gospec/src/gospec"

func TestRedisHashFieldCounterSpecs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(RedisHashFieldCounterSpecs)
	gospec.MainGoTest(r, t)
}

func RedisHashFieldCounterSpecs(c gospec.Context) {

	c.Specify("[RedisHashFieldCounter][Make] Makes new instance", func() {
		value, err := MakeRedisHashFieldCounter(nil, "", "")
		c.Expect(err.Error(), gospec.Equals, "Nil redis connection")
		c.Expect(value, gospec.Satisfies, nil == value)

		value, err = MakeRedisHashFieldCounter(&dog_pool.RedisConnection{}, "", "")
		c.Expect(err.Error(), gospec.Equals, "Empty redis key")
		c.Expect(value, gospec.Satisfies, nil == value)

		value, err = MakeRedisHashFieldCounter(&dog_pool.RedisConnection{}, "Bob", "")
		c.Expect(err.Error(), gospec.Equals, "Empty redis field")
		c.Expect(value, gospec.Satisfies, nil == value)

		value, err = MakeRedisHashFieldCounter(&dog_pool.RedisConnection{}, "Bob", "Field")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, nil != value)
	})

	c.Specify("[RedisHashFieldCounter][String] Formats string", func() {
		value, _ := MakeRedisHashFieldCounter(&dog_pool.RedisConnection{}, "Bob", "Field")
		value.LastValue = nil
		c.Expect(value.String(), gospec.Equals, "Bob[Field] = NaN")

		counter := int64(123)
		value.LastValue = &counter
		c.Expect(value.String(), gospec.Equals, "Bob[Field] = 123")
	})

	c.Specify("[RedisHashFieldCounter][Exists] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashFieldCounter(server.Connection(), "Bob", "Field")

		// Valid number:
		server.Connection().Cmd("HSET", "Bob", "Field", "123")
		ok, err := value.Exists()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ok, gospec.Equals, true)
		c.Expect(value.LastValue, gospec.Satisfies, nil == value.LastValue)

		// Cache Miss
		server.Connection().Cmd("HDEL", "Bob", "Field")
		ok, err = value.Exists()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ok, gospec.Equals, false)
		c.Expect(value.LastValue, gospec.Satisfies, nil == value.LastValue)
	})

	c.Specify("[RedisHashFieldCounter][Int64] Gets value from Redis", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashFieldCounter(server.Connection(), "Bob", "Field")

		// Valid number:
		server.Connection().Cmd("HSET", "Bob", "Field", "123")
		counter, err := value.Int64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, int64(123))

		// Cache Miss
		server.Connection().Cmd("HDEL", "Bob", "Field")
		counter, err = value.Int64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(0))
		c.Expect(value.LastValue, gospec.Satisfies, nil == value.LastValue)

		// Parsing error:
		server.Connection().Cmd("HSET", "Bob", "Field", "Gary")
		counter, err = value.Int64()
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(counter, gospec.Equals, int64(0))
		c.Expect(value.LastValue, gospec.Satisfies, nil == value.LastValue)
	})

	c.Specify("[RedisHashFieldCounter][Get] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashFieldCounter(server.Connection(), "Bob", "Field")

		// Valid number:
		server.Connection().Cmd("HSET", "Bob", "Field", "123")
		counter, err := value.Get()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, int64(123))

		// Cache Miss
		server.Connection().Cmd("HDEL", "Bob", "Field")
		counter, err = value.Get()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(0))
		c.Expect(value.LastValue, gospec.Satisfies, nil == value.LastValue)

		// Parsing error:
		server.Connection().Cmd("HSET", "Bob", "Field", "Gary")
		counter, err = value.Get()
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(counter, gospec.Equals, int64(0))
		c.Expect(value.LastValue, gospec.Satisfies, nil == value.LastValue)
	})

	c.Specify("[RedisHashFieldCounter][Delete] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashFieldCounter(server.Connection(), "Bob", "Field")

		// Valid number:
		server.Connection().Cmd("HSET", "Bob", "Field", "123")
		err := value.Delete()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value.LastValue, gospec.Satisfies, nil == value.LastValue)

		ok, _ := server.Connection().Cmd("HEXISTS", "Bob", "Field").Int()
		c.Expect(ok, gospec.Equals, 0)
	})

	c.Specify("[RedisHashFieldCounter][Set] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashFieldCounter(server.Connection(), "Bob", "Field")

		counter, err := value.Set(123)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, int64(123))

		counter, err = server.Connection().Cmd("HGET", "Bob", "Field").Int64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123))
	})

	c.Specify("[RedisHashFieldCounter][Add] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashFieldCounter(server.Connection(), "Bob", "Field")

		// Valid number:
		server.Connection().Cmd("HSET", "Bob", "Field", "123")
		counter, err := value.Add(555)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123+555))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, int64(123+555))

		counter, err = server.Connection().Cmd("HGET", "Bob", "Field").Int64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123+555))
	})

	c.Specify("[RedisHashFieldCounter][Sub] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashFieldCounter(server.Connection(), "Bob", "Field")

		// Valid number:
		server.Connection().Cmd("HSET", "Bob", "Field", "123")
		counter, err := value.Sub(555)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123-555))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, int64(123-555))

		counter, err = server.Connection().Cmd("HGET", "Bob", "Field").Int64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123-555))
	})

	c.Specify("[RedisHashFieldCounter][Increment] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashFieldCounter(server.Connection(), "Bob", "Field")

		// Valid number:
		server.Connection().Cmd("HSET", "Bob", "Field", "123")
		counter, err := value.Increment()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123+1))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, int64(123+1))

		counter, err = server.Connection().Cmd("HGET", "Bob", "Field").Int64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123+1))
	})

	c.Specify("[RedisHashFieldCounter][Decrement] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashFieldCounter(server.Connection(), "Bob", "Field")

		// Valid number:
		server.Connection().Cmd("HSET", "Bob", "Field", "123")
		counter, err := value.Decrement()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123-1))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, int64(123-1))

		counter, err = server.Connection().Cmd("HGET", "Bob", "Field").Int64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123-1))
	})

}

func Benchmark_RedisHashFieldCounter_Make(b *testing.B) {
	for i := 0; i < b.N; i++ {
		MakeRedisHashFieldCounter(&dog_pool.RedisConnection{}, "Bob", "Field")
	}
}

func Benchmark_RedisHashFieldCounter_String(b *testing.B) {
	value, _ := MakeRedisHashFieldCounter(&dog_pool.RedisConnection{}, "Bob", "Field")
	for i := 0; i < b.N; i++ {
		value.String()
	}
}

func Benchmark_RedisHashFieldCounter_Exists(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounter(server.Connection(), "Bob", "Field")

	// Valid number:
	server.Connection().Cmd("HSET", "Bob", "Field", "123")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Exists()
	}
}

func Benchmark_RedisHashFieldCounter_Int64_ValidNumber(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounter(server.Connection(), "Bob", "Field")

	// Valid number:
	server.Connection().Cmd("HSET", "Bob", "Field", "123")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Int64()
	}
}

func Benchmark_RedisHashFieldCounter_Int64_CacheMiss(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounter(server.Connection(), "Bob", "Field")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Int64()
	}
}

func Benchmark_RedisHashFieldCounter_Int64_InvalidNumber(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounter(server.Connection(), "Bob", "Field")

	// Valid number:
	server.Connection().Cmd("HSET", "Bob", "Field", "Gary")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Int64()
	}
}

func Benchmark_RedisHashFieldCounter_Get(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounter(server.Connection(), "Bob", "Field")

	// Valid number:
	server.Connection().Cmd("HSET", "Bob", "Field", "123")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Get()
	}
}

func Benchmark_RedisHashFieldCounter_Set(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounter(server.Connection(), "Bob", "Field")

	// Valid number:
	server.Connection().Cmd("HSET", "Bob", "Field", "000")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Set(123)
	}
}

func Benchmark_RedisHashFieldCounter_Delete(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounter(server.Connection(), "Bob", "Field")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Delete()
	}
}

func Benchmark_RedisHashFieldCounter_Add(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounter(server.Connection(), "Bob", "Field")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Add(555)
	}
}

func Benchmark_RedisHashFieldCounter_Sub(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounter(server.Connection(), "Bob", "Field")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Sub(555)
	}
}

func Benchmark_RedisHashFieldCounter_Increment(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounter(server.Connection(), "Bob", "Field")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Increment()
	}
}

func Benchmark_RedisHashFieldCounter_Decrement(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounter(server.Connection(), "Bob", "Field")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Decrement()
	}
}
