package redis_counter

import "github.com/alecthomas/log4go"
import "github.com/gnagel/dog_pool/dog_pool"
import "testing"
import "github.com/orfjackal/gospec/src/gospec"

func TestRedisKeyCounterInt64Specs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(RedisKeyCounterInt64Specs)
	gospec.MainGoTest(r, t)
}

func RedisKeyCounterInt64Specs(c gospec.Context) {

	c.Specify("[RedisKeyCounterInt64][Make] Makes new instance", func() {
		value, err := MakeRedisKeyCounterInt64(nil, "")
		c.Expect(err.Error(), gospec.Equals, "Nil redis connection")
		c.Expect(value, gospec.Satisfies, nil == value)

		value, err = MakeRedisKeyCounterInt64(&dog_pool.RedisConnection{}, "")
		c.Expect(err.Error(), gospec.Equals, "Empty redis key")
		c.Expect(value, gospec.Satisfies, nil == value)

		value, err = MakeRedisKeyCounterInt64(&dog_pool.RedisConnection{}, "Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, nil != value)
	})

	c.Specify("[RedisKeyCounterInt64][String] Formats string", func() {
		value, _ := MakeRedisKeyCounterInt64(&dog_pool.RedisConnection{}, "Bob")
		value.LastValue = nil
		c.Expect(value.String(), gospec.Equals, "Bob = NaN")

		counter := int64(123)
		value.LastValue = &counter
		c.Expect(value.String(), gospec.Equals, "Bob = 123")
	})

	c.Specify("[RedisKeyCounterInt64][Exists] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisKeyCounterInt64(server.Connection(), "Bob")

		// Valid number:
		server.Connection().Cmd("SET", "Bob", "123")
		ok, err := value.Exists()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ok, gospec.Equals, true)
		c.Expect(value.LastValue, gospec.Satisfies, nil == value.LastValue)

		// Cache Miss
		server.Connection().Cmd("DEL", "Bob")
		ok, err = value.Exists()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ok, gospec.Equals, false)
		c.Expect(value.LastValue, gospec.Satisfies, nil == value.LastValue)
	})

	c.Specify("[RedisKeyCounterInt64][Int64] Gets value from Redis", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisKeyCounterInt64(server.Connection(), "Bob")

		// Valid number:
		server.Connection().Cmd("SET", "Bob", "123")
		counter, err := value.Int64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, int64(123))

		// Cache Miss
		server.Connection().Cmd("DEL", "Bob")
		counter, err = value.Int64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(0))
		c.Expect(value.LastValue, gospec.Satisfies, nil == value.LastValue)

		// Parsing error:
		server.Connection().Cmd("SET", "Bob", "Gary")
		counter, err = value.Int64()
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(counter, gospec.Equals, int64(0))
		c.Expect(value.LastValue, gospec.Satisfies, nil == value.LastValue)
	})

	c.Specify("[RedisKeyCounterInt64][Get] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisKeyCounterInt64(server.Connection(), "Bob")

		// Valid number:
		server.Connection().Cmd("SET", "Bob", "123")
		counter, err := value.Get()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, int64(123))

		// Cache Miss
		server.Connection().Cmd("DEL", "Bob")
		counter, err = value.Get()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(0))
		c.Expect(value.LastValue, gospec.Satisfies, nil == value.LastValue)

		// Parsing error:
		server.Connection().Cmd("SET", "Bob", "Gary")
		counter, err = value.Get()
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(counter, gospec.Equals, int64(0))
		c.Expect(value.LastValue, gospec.Satisfies, nil == value.LastValue)
	})

	c.Specify("[RedisKeyCounterInt64][Delete] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisKeyCounterInt64(server.Connection(), "Bob")

		// Valid number:
		server.Connection().Cmd("SET", "Bob", "123")
		err := value.Delete()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value.LastValue, gospec.Satisfies, nil == value.LastValue)

		ok, _ := server.Connection().Cmd("EXISTS", "Bob").Int()
		c.Expect(ok, gospec.Equals, 0)
	})

	c.Specify("[RedisKeyCounterInt64][Set] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisKeyCounterInt64(server.Connection(), "Bob")

		counter, err := value.Set(123)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, int64(123))

		counter, err = server.Connection().Cmd("GET", "Bob").Int64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123))
	})

	c.Specify("[RedisKeyCounterInt64][Add] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisKeyCounterInt64(server.Connection(), "Bob")

		// Valid number:
		server.Connection().Cmd("SET", "Bob", "123")
		counter, err := value.Add(555)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123+555))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, int64(123+555))

		counter, err = server.Connection().Cmd("GET", "Bob").Int64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123+555))
	})

	c.Specify("[RedisKeyCounterInt64][Sub] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisKeyCounterInt64(server.Connection(), "Bob")

		// Valid number:
		server.Connection().Cmd("SET", "Bob", "123")
		counter, err := value.Sub(555)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123-555))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, int64(123-555))

		counter, err = server.Connection().Cmd("GET", "Bob").Int64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123-555))
	})

	c.Specify("[RedisKeyCounterInt64][Increment] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisKeyCounterInt64(server.Connection(), "Bob")

		// Valid number:
		server.Connection().Cmd("SET", "Bob", "123")
		counter, err := value.Increment()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123+1))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, int64(123+1))

		counter, err = server.Connection().Cmd("GET", "Bob").Int64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123+1))
	})

	c.Specify("[RedisKeyCounterInt64][Decrement] Redis Operation", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, server_err := dog_pool.StartRedisServer(&logger)
		if nil != server_err {
			panic(server_err)
		}
		defer server.Close()

		value, _ := MakeRedisKeyCounterInt64(server.Connection(), "Bob")

		// Valid number:
		server.Connection().Cmd("SET", "Bob", "123")
		counter, err := value.Decrement()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123-1))
		c.Expect(value.LastValue, gospec.Satisfies, nil != value.LastValue)
		c.Expect(*value.LastValue, gospec.Equals, int64(123-1))

		counter, err = server.Connection().Cmd("GET", "Bob").Int64()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(counter, gospec.Equals, int64(123-1))
	})

}

func Benchmark_RedisKeyCounterInt64_Make(b *testing.B) {
	for i := 0; i < b.N; i++ {
		MakeRedisKeyCounterInt64(&dog_pool.RedisConnection{}, "Bob")
	}
}

func Benchmark_RedisKeyCounterInt64_String(b *testing.B) {
	value, _ := MakeRedisKeyCounterInt64(&dog_pool.RedisConnection{}, "Bob")
	for i := 0; i < b.N; i++ {
		value.String()
	}
}

func Benchmark_RedisKeyCounterInt64_Exists(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisKeyCounterInt64(server.Connection(), "Bob")

	// Valid number:
	server.Connection().Cmd("SET", "Bob", "123")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Exists()
	}
}

func Benchmark_RedisKeyCounterInt64_Int64_ValidNumber(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisKeyCounterInt64(server.Connection(), "Bob")

	// Valid number:
	server.Connection().Cmd("SET", "Bob", "123")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Int64()
	}
}

func Benchmark_RedisKeyCounterInt64_Int64_CacheMiss(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisKeyCounterInt64(server.Connection(), "Bob")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Int64()
	}
}

func Benchmark_RedisKeyCounterInt64_Int64_InvalidNumber(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisKeyCounterInt64(server.Connection(), "Bob")

	// Valid number:
	server.Connection().Cmd("SET", "Bob", "Gary")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Int64()
	}
}

func Benchmark_RedisKeyCounterInt64_Get(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisKeyCounterInt64(server.Connection(), "Bob")

	// Valid number:
	server.Connection().Cmd("SET", "Bob", "123")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Get()
	}
}

func Benchmark_RedisKeyCounterInt64_Set(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisKeyCounterInt64(server.Connection(), "Bob")

	// Valid number:
	server.Connection().Cmd("SET", "Bob", "000")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Set(123)
	}
}

func Benchmark_RedisKeyCounterInt64_Delete(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisKeyCounterInt64(server.Connection(), "Bob")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Delete()
	}
}

func Benchmark_RedisKeyCounterInt64_Add(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisKeyCounterInt64(server.Connection(), "Bob")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Add(555)
	}
}

func Benchmark_RedisKeyCounterInt64_Sub(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisKeyCounterInt64(server.Connection(), "Bob")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Sub(555)
	}
}

func Benchmark_RedisKeyCounterInt64_Increment(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisKeyCounterInt64(server.Connection(), "Bob")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Increment()
	}
}

func Benchmark_RedisKeyCounterInt64_Decrement(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	value, _ := MakeRedisKeyCounterInt64(server.Connection(), "Bob")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value.Decrement()
	}
}
