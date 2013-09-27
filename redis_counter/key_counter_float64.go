package redis_counter

import "fmt"
import "github.com/gnagel/dog_pool/dog_pool"

type RedisKeyCounterFloat64 struct {
	Redis     dog_pool.RedisClientInterface
	KEY       string
	LastValue *float64
}

// Make a new instance of RedisKeyCounterFloat64
func MakeRedisKeyCounterFloat64(redis dog_pool.RedisClientInterface, key string) (*RedisKeyCounterFloat64, error) {
	switch {
	case nil == redis:
		return nil, fmt.Errorf("Nil redis connection")
	case len(key) == 0:
		return nil, fmt.Errorf("Empty redis key")
	default:
		return &RedisKeyCounterFloat64{redis, key, nil}, nil
	}
}

// Format the value as a string; uses the cached "LastValue" field
func (p *RedisKeyCounterFloat64) String() string {
	switch p.LastValue {
	case nil:
		return fmt.Sprintf("%s = NaN", p.KEY)
	default:
		return fmt.Sprintf("%s = %0.6f", p.KEY, *p.LastValue)
	}
}

// Get the value of the counter; saves the counter to "LastValue"
func (p *RedisKeyCounterFloat64) Float64() (float64, error) {
	return p.operationReturnsAmount("GET")
}

func (p *RedisKeyCounterFloat64) Exists() (bool, error) {
	p.LastValue = nil
	reply := p.Redis.Cmd("EXISTS", p.KEY)
	if nil != reply.Err {
		return false, reply.Err
	}

	ok, err := reply.Int()
	if err != nil {
		return false, err
	}

	return ok == 1, nil
}

func (p *RedisKeyCounterFloat64) Delete() error {
	p.LastValue = nil
	reply := p.Redis.Cmd("DEL", p.KEY)
	return reply.Err
}

func (p *RedisKeyCounterFloat64) Get() (float64, error) {
	return p.Float64()
}

func (p *RedisKeyCounterFloat64) Set(amount float64) (float64, error) {
	return p.operationReplacesAmount("SET", amount)
}

func (p *RedisKeyCounterFloat64) Add(amount float64) (float64, error) {
	return p.operationModifiesAmount("INCRBYFLOAT", amount)
}

func (p *RedisKeyCounterFloat64) Sub(amount float64) (float64, error) {
	return p.Add(-1 * amount)
}

func (p *RedisKeyCounterFloat64) Increment() (float64, error) {
	return p.Add(1.0)
}

func (p *RedisKeyCounterFloat64) Decrement() (float64, error) {
	return p.Sub(1.0)
}

//
// Internal Helpers:
//

func (p *RedisKeyCounterFloat64) operationReturnsAmount(cmd string) (float64, error) {
	p.LastValue = nil
	reply := p.Redis.Cmd(cmd, p.KEY)
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

func (p *RedisKeyCounterFloat64) operationModifiesAmount(cmd string, amount float64) (float64, error) {
	p.LastValue = nil
	reply := p.Redis.Cmd(cmd, p.KEY, amount)
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

func (p *RedisKeyCounterFloat64) operationReplacesAmount(cmd string, amount float64) (float64, error) {
	p.LastValue = nil
	reply := p.Redis.Cmd(cmd, p.KEY, amount)
	switch {
	case nil != reply.Err:
		return 0, reply.Err
	default:
		p.LastValue = &amount
		return amount, nil
	}
}
