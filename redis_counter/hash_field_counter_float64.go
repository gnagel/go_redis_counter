package redis_counter

import "fmt"
import "github.com/gnagel/dog_pool/dog_pool"

type RedisHashFieldCounterFloat64 struct {
	Redis      dog_pool.RedisClientInterface
	KEY, FIELD string
	LastValue  *float64
}

// Make a new instance of RedisHashFieldCounterFloat64
func MakeRedisHashFieldCounterFloat64(redis dog_pool.RedisClientInterface, key, field string) (*RedisHashFieldCounterFloat64, error) {
	switch {
	case nil == redis:
		return nil, fmt.Errorf("Nil redis connection")
	case len(key) == 0:
		return nil, fmt.Errorf("Empty redis key")
	case len(field) == 0:
		return nil, fmt.Errorf("Empty redis field")
	default:
		return &RedisHashFieldCounterFloat64{redis, key, field, nil}, nil
	}
}

// Format the value as a string; uses the cached "LastValue" field
func (p *RedisHashFieldCounterFloat64) String() string {
	switch p.LastValue {
	case nil:
		return fmt.Sprintf("%s[%s] = NaN", p.KEY, p.FIELD)
	default:
		return fmt.Sprintf("%s[%s] = %0.6f", p.KEY, p.FIELD, *p.LastValue)
	}
}

// Get the value of the counter; saves the counter to "LastValue"
func (p *RedisHashFieldCounterFloat64) Float64() (float64, error) {
	return p.operationReturnsAmount("HGET")
}

func (p *RedisHashFieldCounterFloat64) Exists() (bool, error) {
	p.LastValue = nil
	reply := p.Redis.Cmd("HEXISTS", p.KEY, p.FIELD)
	if nil != reply.Err {
		return false, reply.Err
	}

	ok, err := reply.Int()
	if err != nil {
		return false, err
	}

	return ok == 1, nil
}

func (p *RedisHashFieldCounterFloat64) Delete() error {
	p.LastValue = nil
	reply := p.Redis.Cmd("HDEL", p.KEY, p.FIELD)
	return reply.Err
}

func (p *RedisHashFieldCounterFloat64) Get() (float64, error) {
	return p.Float64()
}

func (p *RedisHashFieldCounterFloat64) Set(amount float64) (float64, error) {
	return p.operationReplacesAmount("HSET", amount)
}

func (p *RedisHashFieldCounterFloat64) Add(amount float64) (float64, error) {
	return p.operationModifiesAmount("HINCRBYFLOAT", amount)
}

func (p *RedisHashFieldCounterFloat64) Sub(amount float64) (float64, error) {
	return p.Add(-1 * amount)
}

func (p *RedisHashFieldCounterFloat64) Increment() (float64, error) {
	return p.Add(1)
}

func (p *RedisHashFieldCounterFloat64) Decrement() (float64, error) {
	return p.Sub(1)
}

//
// Internal Helpers:
//

func (p *RedisHashFieldCounterFloat64) operationReturnsAmount(cmd string) (float64, error) {
	p.LastValue = nil
	reply := p.Redis.Cmd(cmd, p.KEY, p.FIELD)
	ptr, err := toFloat64Ptr(reply)
	switch {
	case nil != err:
		return 0, err
	case nil != ptr:
		p.LastValue = ptr
		return *ptr, nil
	default:
		return 0, nil
	}
}

func (p *RedisHashFieldCounterFloat64) operationModifiesAmount(cmd string, amount float64) (float64, error) {
	p.LastValue = nil
	reply := p.Redis.Cmd(cmd, p.KEY, p.FIELD, amount)
	ptr, err := toFloat64Ptr(reply)
	switch {
	case nil != err:
		return 0, err
	case nil != ptr:
		p.LastValue = ptr
		return *ptr, nil
	default:
		return 0, nil
	}
}

func (p *RedisHashFieldCounterFloat64) operationReplacesAmount(cmd string, amount float64) (float64, error) {
	p.LastValue = nil
	reply := p.Redis.Cmd(cmd, p.KEY, p.FIELD, amount)
	switch {
	case nil != reply.Err:
		return 0, reply.Err
	default:
		p.LastValue = &amount
		return amount, nil
	}
}
