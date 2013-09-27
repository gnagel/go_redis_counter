package redis_counter

import "github.com/alecthomas/log4go"
import "github.com/gnagel/dog_pool/dog_pool"
import "testing"
import "github.com/orfjackal/gospec/src/gospec"

func TestRedisHashFieldCounterFloat64Specs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(RedisHashFieldCounterFloat64Specs)
	gospec.MainGoTest(r, t)
}

func RedisHashFieldCounterFloat64Specs(c gospec.Context) {

	c.Specify("[RedisHashFieldCounterFloat64][Make] Makes new instance", func() {
		value, err := MakeRedisHashFieldCounterFloat64(nil, "", "")
		c.Expect(err.Error(), gospec.Equals, "Nil redis connection")
		c.Expect(value, gospec.Satisfies, nil == value)

		value, err = MakeRedisHashFieldCounterFloat64(&dog_pool.RedisConnection{}, "", "")
		c.Expect(err.Error(), gospec.Equals, "Empty redis key")
		c.Expect(value, gospec.Satisfies, nil == value)

		value, err = MakeRedisHashFieldCounterFloat64(&dog_pool.RedisConnection{}, "Bob", "")
		c.Expect(err.Error(), gospec.Equals, "Empty redis field")
		c.Expect(value, gospec.Satisfies, nil == value)

		value, err = MakeRedisHashFieldCounterFloat64(&dog_pool.RedisConnection{}, "Bob", "Field")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, nil != value)
	})

	c.Specify("[RedisHashFieldCounterFloat64][String] Formats string", func() {
		value, _ := MakeRedisHashFieldCounterFloat64(&dog_pool.RedisConnection{}, "Bob", "Field")
		value.LastValue = nil
		c.Expect(value.String(), gospec.Equals, "Bob[Field] = NaN")

		counter := float64(123.456)
		value.LastValue = &counter
		c.Expect(value.String(), gospec.Equals, "Bob[Field] = 123.456000")
	})

	c.Specify("[RedisHashFieldCounterFloat64][Exists] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashFieldCounterFloat64(server.Connection(), "Bob", "Field")

		// Valid number:
		server.Connection().Cmd("HSET", "Bob", "Field", "123.456")
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

	c.Specify("[RedisHashFieldCounterFloat64][Float64] Gets value from Redis", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashFieldCounterFloat64(server.Connection(), "Bob", "Field")

		// Valid number:
		server.Connection().Cmd("HSET", "Bob", "Field", "123.456")
		counter, err := value.Float64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, float64(123.456))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, float64(123.456))

		// Cache Miss
		server.Connection().Cmd("HDEL", "Bob", "Field")
		counter, err = value.Float64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, float64(0))
		c.Expect(value.LastValue, gospec.Satisfies, nil == value.LastValue)

		// Parsing error:
		server.Connection().Cmd("HSET", "Bob", "Field", "Gary")
		counter, err = value.Float64()
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(counter, gospec.Equals, float64(0))
		c.Expect(value.LastValue, gospec.Satisfies, nil == value.LastValue)
	})

	c.Specify("[RedisHashFieldCounterFloat64][Get] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashFieldCounterFloat64(server.Connection(), "Bob", "Field")

		// Valid number:
		server.Connection().Cmd("HSET", "Bob", "Field", "123.456")
		counter, err := value.Get()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, float64(123.456))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, float64(123.456))

		// Cache Miss
		server.Connection().Cmd("HDEL", "Bob", "Field")
		counter, err = value.Get()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, float64(0))
		c.Expect(value.LastValue, gospec.Satisfies, nil == value.LastValue)

		// Parsing error:
		server.Connection().Cmd("HSET", "Bob", "Field", "Gary")
		counter, err = value.Get()
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(counter, gospec.Equals, float64(0))
		c.Expect(value.LastValue, gospec.Satisfies, nil == value.LastValue)
	})

	c.Specify("[RedisHashFieldCounterFloat64][Delete] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashFieldCounterFloat64(server.Connection(), "Bob", "Field")

		// Valid number:
		server.Connection().Cmd("HSET", "Bob", "Field", "123.456")
		err := value.Delete()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value.LastValue, gospec.Satisfies, nil == value.LastValue)

		ok, _ := server.Connection().Cmd("HEXISTS", "Bob", "Field").Int()
		c.Expect(ok, gospec.Equals, 0)
	})

	c.Specify("[RedisHashFieldCounterFloat64][Set] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashFieldCounterFloat64(server.Connection(), "Bob", "Field")

		counter, err := value.Set(123.456)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, float64(123.456))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, float64(123.456))

		str, _ := server.Connection().Cmd("HGET", "Bob", "Field").Str()
		c.Expect(str, gospec.Equals, "123.456")
	})

	c.Specify("[RedisHashFieldCounterFloat64][Add] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashFieldCounterFloat64(server.Connection(), "Bob", "Field")

		// Valid number:
		server.Connection().Cmd("HSET", "Bob", "Field", "123.456")
		counter, err := value.Add(555)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, float64(123.456+555))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, float64(123.456+555))

		str, _ := server.Connection().Cmd("HGET", "Bob", "Field").Str()
		c.Expect(str, gospec.Equals, "678.45600000000000002")
	})

	c.Specify("[RedisHashFieldCounterFloat64][Sub] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashFieldCounterFloat64(server.Connection(), "Bob", "Field")

		// Valid number:
		server.Connection().Cmd("HSET", "Bob", "Field", "123.456")
		counter, err := value.Sub(555)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, float64(123.456-555))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, float64(123.456-555))

		str, _ := server.Connection().Cmd("HGET", "Bob", "Field").Str()
		c.Expect(str, gospec.Equals, "-431.54399999999999998")
	})

	c.Specify("[RedisHashFieldCounterFloat64][Increment] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashFieldCounterFloat64(server.Connection(), "Bob", "Field")

		// Valid number:
		server.Connection().Cmd("HSET", "Bob", "Field", "123.456")
		counter, err := value.Increment()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, float64(123.456+1))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, float64(123.456+1))

		str, _ := server.Connection().Cmd("HGET", "Bob", "Field").Str()
		c.Expect(str, gospec.Equals, "124.456")
	})

	c.Specify("[RedisHashFieldCounterFloat64][Decrement] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisHashFieldCounterFloat64(server.Connection(), "Bob", "Field")

		// Valid number:
		server.Connection().Cmd("HSET", "Bob", "Field", "123.456")
		counter, err := value.Decrement()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, float64(123.456-1))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, float64(123.456-1))

		str, _ := server.Connection().Cmd("HGET", "Bob", "Field").Str()
		c.Expect(str, gospec.Equals, "122.456")
	})

}

func Benchmark_RedisHashFieldCounterFloat64_Make(b *testing.B) {
	for i := 0; i < b.N; i++ {
		MakeRedisHashFieldCounterFloat64(&dog_pool.RedisConnection{}, "Bob", "Field")
	}
}

func Benchmark_RedisHashFieldCounterFloat64_String(b *testing.B) {
	value, _ := MakeRedisHashFieldCounterFloat64(&dog_pool.RedisConnection{}, "Bob", "Field")
	for i := 0; i < b.N; i++ {
		value.String()
	}
}

func Benchmark_RedisHashFieldCounterFloat64_Exists(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounterFloat64(server.Connection(), "Bob", "Field")

	// Valid number:
	server.Connection().Cmd("HSET", "Bob", "Field", "123.456")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Exists()
	}
}

func Benchmark_RedisHashFieldCounterFloat64_Float64_ValidNumber(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounterFloat64(server.Connection(), "Bob", "Field")

	// Valid number:
	server.Connection().Cmd("HSET", "Bob", "Field", "123.456")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Float64()
	}
}

func Benchmark_RedisHashFieldCounterFloat64_Float64_CacheMiss(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounterFloat64(server.Connection(), "Bob", "Field")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Float64()
	}
}

func Benchmark_RedisHashFieldCounterFloat64_Float64_InvalidNumber(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounterFloat64(server.Connection(), "Bob", "Field")

	// Valid number:
	server.Connection().Cmd("HSET", "Bob", "Field", "Gary")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Float64()
	}
}

func Benchmark_RedisHashFieldCounterFloat64_Get(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounterFloat64(server.Connection(), "Bob", "Field")

	// Valid number:
	server.Connection().Cmd("HSET", "Bob", "Field", "123.456")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Get()
	}
}

func Benchmark_RedisHashFieldCounterFloat64_Set(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounterFloat64(server.Connection(), "Bob", "Field")

	// Valid number:
	server.Connection().Cmd("HSET", "Bob", "Field", "000")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Set(123.456)
	}
}

func Benchmark_RedisHashFieldCounterFloat64_Delete(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounterFloat64(server.Connection(), "Bob", "Field")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Delete()
	}
}

func Benchmark_RedisHashFieldCounterFloat64_Add(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounterFloat64(server.Connection(), "Bob", "Field")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Add(555)
	}
}

func Benchmark_RedisHashFieldCounterFloat64_Sub(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounterFloat64(server.Connection(), "Bob", "Field")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Sub(555)
	}
}

func Benchmark_RedisHashFieldCounterFloat64_Increment(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounterFloat64(server.Connection(), "Bob", "Field")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Increment()
	}
}

func Benchmark_RedisHashFieldCounterFloat64_Decrement(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisHashFieldCounterFloat64(server.Connection(), "Bob", "Field")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Decrement()
	}
}
