package redis_counter

import "fmt"
import "github.com/gnagel/dog_pool/dog_pool"
import "github.com/fzzy/radix/redis"

type RedisKeyCounter struct {
	Redis     dog_pool.RedisClientInterface
	KEY       string
	LastValue *int64
}

// Make a new instance of RedisKeyCounter
func MakeRedisKeyCounter(redis dog_pool.RedisClientInterface, key string) (*RedisKeyCounter, error) {
	switch {
	case nil == redis:
		return nil, fmt.Errorf("Nil redis connection")
	case len(key) == 0:
		return nil, fmt.Errorf("Empty redis key")
	default:
		return &RedisKeyCounter{redis, key, nil}, nil
	}
}

// Format the value as a string; uses the cached "LastValue" field
func (p *RedisKeyCounter) String() string {
	switch p.LastValue {
	case nil:
		return fmt.Sprintf("%s = NaN", p.KEY)
	default:
		return fmt.Sprintf("%s = %d", p.KEY, *p.LastValue)
	}
}

// Get the value of the counter; saves the counter to "LastValue"
func (p *RedisKeyCounter) Int64() (int64, error) {
	return p.operationReturnsAmount("GET")
}

func (p *RedisKeyCounter) Exists() (bool, error) {
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

func (p *RedisKeyCounter) Delete() error {
	p.LastValue = nil
	reply := p.Redis.Cmd("DEL", p.KEY)
	return reply.Err
}

func (p *RedisKeyCounter) Get() (int64, error) {
	return p.Int64()
}

func (p *RedisKeyCounter) Set(amount int64) (int64, error) {
	return p.operationReplacesAmount("SET", amount)
}

func (p *RedisKeyCounter) Add(amount int64) (int64, error) {
	return p.operationModifiesAmount("INCRBY", amount)
}

func (p *RedisKeyCounter) Sub(amount int64) (int64, error) {
	return p.operationModifiesAmount("DECRBY", amount)
}

func (p *RedisKeyCounter) Increment() (int64, error) {
	return p.operationReturnsAmount("INCR")
}

func (p *RedisKeyCounter) Decrement() (int64, error) {
	return p.operationReturnsAmount("DECR")
}

//
// Internal Helpers:
//

func (p *RedisKeyCounter) operationReturnsAmount(cmd string) (int64, error) {
	p.LastValue = nil
	reply := p.Redis.Cmd(cmd, p.KEY)
	switch {
	case nil != reply.Err:
		return 0, reply.Err
	case redis.NilReply == reply.Type:
		return 0, nil
	default:
		value, err := reply.Int64()
		if nil != err {
			return 0, err
		}

		p.LastValue = &value
		return value, nil
	}
}

func (p *RedisKeyCounter) operationModifiesAmount(cmd string, amount int64) (int64, error) {
	p.LastValue = nil
	reply := p.Redis.Cmd(cmd, p.KEY, amount)
	switch {
	case nil != reply.Err:
		return 0, reply.Err
	default:
		value, err := reply.Int64()
		if nil != err {
			return 0, err
		}

		p.LastValue = &value
		return value, nil
	}
}

func (p *RedisKeyCounter) operationReplacesAmount(cmd string, amount int64) (int64, error) {
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
