package redis_counter

import "fmt"
import "github.com/gnagel/dog_pool/dog_pool"

type RedisHashFieldCounterInt64 struct {
	Redis      dog_pool.RedisClientInterface
	KEY, FIELD string
	LastValue  *int64
}

// Make a new instance of RedisHashFieldCounterInt64
func MakeRedisHashFieldCounterInt64(redis dog_pool.RedisClientInterface, key, field string) (*RedisHashFieldCounterInt64, error) {
	switch {
	case nil == redis:
		return nil, fmt.Errorf("Nil redis connection")
	case len(key) == 0:
		return nil, fmt.Errorf("Empty redis key")
	case len(field) == 0:
		return nil, fmt.Errorf("Empty redis field")
	default:
		return &RedisHashFieldCounterInt64{redis, key, field, nil}, nil
	}
}

// Format the value as a string; uses the cached "LastValue" field
func (p *RedisHashFieldCounterInt64) String() string {
	switch p.LastValue {
	case nil:
		return fmt.Sprintf("%s[%s] = NaN", p.KEY, p.FIELD)
	default:
		return fmt.Sprintf("%s[%s] = %d", p.KEY, p.FIELD, *p.LastValue)
	}
}

// Get the value of the counter; saves the counter to "LastValue"
func (p *RedisHashFieldCounterInt64) Int64() (int64, error) {
	return p.operationReturnsAmount("HGET")
}

func (p *RedisHashFieldCounterInt64) Exists() (bool, error) {
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

func (p *RedisHashFieldCounterInt64) Delete() error {
	p.LastValue = nil
	reply := p.Redis.Cmd("HDEL", p.KEY, p.FIELD)
	return reply.Err
}

func (p *RedisHashFieldCounterInt64) Get() (int64, error) {
	return p.Int64()
}

func (p *RedisHashFieldCounterInt64) Set(amount int64) (int64, error) {
	return p.operationReplacesAmount("HSET", amount)
}

func (p *RedisHashFieldCounterInt64) Add(amount int64) (int64, error) {
	return p.operationModifiesAmount("HINCRBY", amount)
}

func (p *RedisHashFieldCounterInt64) Sub(amount int64) (int64, error) {
	return p.Add(-1 * amount)
}

func (p *RedisHashFieldCounterInt64) Increment() (int64, error) {
	return p.Add(1)
}

func (p *RedisHashFieldCounterInt64) Decrement() (int64, error) {
	return p.Sub(1)
}

//
// Internal Helpers:
//

func (p *RedisHashFieldCounterInt64) operationReturnsAmount(cmd string) (int64, error) {
	p.LastValue = nil
	reply := p.Redis.Cmd(cmd, p.KEY, p.FIELD)
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

func (p *RedisHashFieldCounterInt64) operationModifiesAmount(cmd string, amount int64) (int64, error) {
	p.LastValue = nil
	reply := p.Redis.Cmd(cmd, p.KEY, p.FIELD, amount)
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

func (p *RedisHashFieldCounterInt64) operationReplacesAmount(cmd string, amount int64) (int64, error) {
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
