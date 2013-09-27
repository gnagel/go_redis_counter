package redis_counter

import "fmt"
import "github.com/gnagel/dog_pool/dog_pool"
import "github.com/fzzy/radix/redis"

type RedisHashFieldCounter struct {
	Redis      dog_pool.RedisClientInterface
	KEY, FIELD string
	LastValue  *int64
}

// Make a new instance of RedisHashFieldCounter
func MakeRedisHashFieldCounter(redis dog_pool.RedisClientInterface, key, field string) (*RedisHashFieldCounter, error) {
	switch {
	case nil == redis:
		return nil, fmt.Errorf("Nil redis connection")
	case len(key) == 0:
		return nil, fmt.Errorf("Empty redis key")
	case len(field) == 0:
		return nil, fmt.Errorf("Empty redis field")
	default:
		return &RedisHashFieldCounter{redis, key, field, nil}, nil
	}
}

// Format the value as a string; uses the cached "LastValue" field
func (p *RedisHashFieldCounter) String() string {
	switch p.LastValue {
	case nil:
		return fmt.Sprintf("%s[%s] = NaN", p.KEY, p.FIELD)
	default:
		return fmt.Sprintf("%s[%s] = %d", p.KEY, p.FIELD, *p.LastValue)
	}
}

// Get the value of the counter; saves the counter to "LastValue"
func (p *RedisHashFieldCounter) Int64() (int64, error) {
	return p.operationReturnsAmount("HGET")
}

func (p *RedisHashFieldCounter) Exists() (bool, error) {
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

func (p *RedisHashFieldCounter) Delete() error {
	p.LastValue = nil
	reply := p.Redis.Cmd("HDEL", p.KEY, p.FIELD)
	return reply.Err
}

func (p *RedisHashFieldCounter) Get() (int64, error) {
	return p.Int64()
}

func (p *RedisHashFieldCounter) Set(amount int64) (int64, error) {
	return p.operationReplacesAmount("HSET", amount)
}

func (p *RedisHashFieldCounter) Add(amount int64) (int64, error) {
	return p.operationModifiesAmount("HINCRBY", amount)
}

func (p *RedisHashFieldCounter) Sub(amount int64) (int64, error) {
	return p.Add(-1 * amount)
}

func (p *RedisHashFieldCounter) Increment() (int64, error) {
	return p.Add(1)
}

func (p *RedisHashFieldCounter) Decrement() (int64, error) {
	return p.Sub(1)
}

//
// Internal Helpers:
//

func (p *RedisHashFieldCounter) operationReturnsAmount(cmd string) (int64, error) {
	p.LastValue = nil
	reply := p.Redis.Cmd(cmd, p.KEY, p.FIELD)
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

func (p *RedisHashFieldCounter) operationModifiesAmount(cmd string, amount int64) (int64, error) {
	p.LastValue = nil
	reply := p.Redis.Cmd(cmd, p.KEY, p.FIELD, amount)
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

func (p *RedisHashFieldCounter) operationReplacesAmount(cmd string, amount int64) (int64, error) {
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
