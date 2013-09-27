package redis_counter

import "fmt"
import "github.com/gnagel/dog_pool/dog_pool"

type RedisKeyCounterInt64 struct {
	Redis     dog_pool.RedisClientInterface
	KEY       string
	LastValue *int64
}

// Make a new instance of RedisKeyCounterInt64
func MakeRedisKeyCounterInt64(redis dog_pool.RedisClientInterface, key string) (*RedisKeyCounterInt64, error) {
	switch {
	case nil == redis:
		return nil, fmt.Errorf("Nil redis connection")
	case len(key) == 0:
		return nil, fmt.Errorf("Empty redis key")
	default:
		return &RedisKeyCounterInt64{redis, key, nil}, nil
	}
}

// Format the value as a string; uses the cached "LastValue" field
func (p *RedisKeyCounterInt64) String() string {
	switch p.LastValue {
	case nil:
		return fmt.Sprintf("%s = NaN", p.KEY)
	default:
		return fmt.Sprintf("%s = %d", p.KEY, *p.LastValue)
	}
}

// Get the value of the counter; saves the counter to "LastValue"
func (p *RedisKeyCounterInt64) Int64() (int64, error) {
	return p.operationReturnsAmount("GET")
}

func (p *RedisKeyCounterInt64) Exists() (bool, error) {
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

func (p *RedisKeyCounterInt64) Delete() error {
	p.LastValue = nil
	reply := p.Redis.Cmd("DEL", p.KEY)
	return reply.Err
}

func (p *RedisKeyCounterInt64) Get() (int64, error) {
	return p.Int64()
}

func (p *RedisKeyCounterInt64) Set(amount int64) (int64, error) {
	return p.operationReplacesAmount("SET", amount)
}

func (p *RedisKeyCounterInt64) Add(amount int64) (int64, error) {
	return p.operationModifiesAmount("INCRBY", amount)
}

func (p *RedisKeyCounterInt64) Sub(amount int64) (int64, error) {
	return p.operationModifiesAmount("DECRBY", amount)
}

func (p *RedisKeyCounterInt64) Increment() (int64, error) {
	return p.operationReturnsAmount("INCR")
}

func (p *RedisKeyCounterInt64) Decrement() (int64, error) {
	return p.operationReturnsAmount("DECR")
}

//
// Internal Helpers:
//

func (p *RedisKeyCounterInt64) operationReturnsAmount(cmd string) (int64, error) {
	p.LastValue = nil
	reply := p.Redis.Cmd(cmd, p.KEY)
	ptr, err := toInt64Ptr(reply)
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

func (p *RedisKeyCounterInt64) operationModifiesAmount(cmd string, amount int64) (int64, error) {
	p.LastValue = nil
	reply := p.Redis.Cmd(cmd, p.KEY, amount)
	ptr, err := toInt64Ptr(reply)
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

func (p *RedisKeyCounterInt64) operationReplacesAmount(cmd string, amount int64) (int64, error) {
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
