package redis_counter

import "fmt"
import "github.com/gnagel/dog_pool/dog_pool"

type RedisMKeysCounterInt64 struct {
	Redis dog_pool.RedisClientInterface
	KEYS  []string
	cache *PtrMapInt64
}

// Make a new instance of RedisMKeysCounterInt64
func MakeRedisMKeysCounterInt64(redis dog_pool.RedisClientInterface, keys ...string) (*RedisMKeysCounterInt64, error) {
	switch {
	case nil == redis:
		return nil, fmt.Errorf("Nil redis connection")
	case len(keys) == 0:
		return nil, fmt.Errorf("Empty redis keys")
	default:
		for i, key := range keys {
			if len(key) == 0 {
				return nil, fmt.Errorf("Empty redis key[%d]", i)
			}
		}

		return &RedisMKeysCounterInt64{redis, keys, makePtrMapInt64(len(keys))}, nil
	}
}

func (p *RedisMKeysCounterInt64) LastValue(key string) *int64 {
	return p.cache.Value(key)
}

// Format the values as a string; uses the cached "LastValues" field
func (p *RedisMKeysCounterInt64) String() string {
	return p.cache.String(p.KEYS)
}

// Get the value of the counters; saves the counter to "LastValues"
func (p *RedisMKeysCounterInt64) MInt64() ([]int64, error) {
	return p.operationReturnsAmounts("MGET")
}

func (p *RedisMKeysCounterInt64) MExists() ([]bool, error) {
	p.cache.Reset()

	count := len(p.KEYS)
	commands := make([]*dog_pool.RedisBatchCommand, count)
	for i, key := range p.KEYS {
		commands[i] = dog_pool.MakeRedisBatchCommandExists(key)
	}

	err := dog_pool.RedisBatchCommands(commands).ExecuteBatch(p.Redis)
	if err != nil {
		return nil, err
	}

	exists := make([]bool, count)
	for i := range p.KEYS {
		reply := commands[i].Reply()
		if nil != reply.Err {
			return nil, reply.Err
		}

		ok, err := reply.Int()
		if err != nil {
			return nil, err
		}

		exists[i] = ok == 1
	}

	return exists, nil
}

func (p *RedisMKeysCounterInt64) MDelete() error {
	p.cache.Reset()
	reply := p.Redis.Cmd("DEL", p.KEYS)
	return reply.Err
}

func (p *RedisMKeysCounterInt64) MGet() ([]int64, error) {
	return p.MInt64()
}

func (p *RedisMKeysCounterInt64) MSet(amount int64) ([]int64, error) {
	return p.operationReplacesAmounts(amount)
}

func (p *RedisMKeysCounterInt64) MAdd(amount int64) ([]int64, error) {
	return p.operationModifiesAmounts("INCRBY", amount)
}

func (p *RedisMKeysCounterInt64) MSub(amount int64) ([]int64, error) {
	return p.operationModifiesAmounts("DECRBY", amount)
}

func (p *RedisMKeysCounterInt64) MIncrement() ([]int64, error) {
	return p.operationReturnsAmount("INCR")
}

func (p *RedisMKeysCounterInt64) MDecrement() ([]int64, error) {
	return p.operationReturnsAmount("DECR")
}

//
// Internal Helpers:
//

func (p *RedisMKeysCounterInt64) operationReturnsAmounts(cmd string) ([]int64, error) {
	p.cache.Reset()
	count := len(p.KEYS)

	reply := p.Redis.Cmd(cmd, p.KEYS)
	switch {
	case nil != reply.Err:
		return nil, reply.Err
	default:
		values := make([]int64, count)
		for i, key := range p.KEYS {
			ptr, err := toInt64Ptr(reply.Elems[i])
			switch {
			case nil != err:
				return nil, err
			case nil != ptr:
				p.cache.Set(key, ptr)
				values[i] = *ptr
			}
		}

		return values, nil
	}
}

func (p *RedisMKeysCounterInt64) operationReturnsAmount(cmd string) ([]int64, error) {
	p.cache.Reset()
	count := len(p.KEYS)

	commands := make([]*dog_pool.RedisBatchCommand, count)
	for i, key := range p.KEYS {
		commands[i] = dog_pool.MakeRedisBatchCommand(cmd)
		commands[i].WriteStringArg(key)
	}

	err := dog_pool.RedisBatchCommands(commands).ExecuteBatch(p.Redis)
	if err != nil {
		return nil, err
	}

	values := make([]int64, count)
	for i, key := range p.KEYS {
		ptr, err := toInt64Ptr(commands[i].Reply())
		switch {
		case nil != err:
			return nil, err
		case nil != ptr:
			p.cache.Set(key, ptr)
			values[i] = *ptr
		}
	}

	return values, nil
}

func (p *RedisMKeysCounterInt64) operationModifiesAmounts(cmd string, amount int64) ([]int64, error) {
	p.cache.Reset()

	count := len(p.KEYS)
	amount_bytes := []byte(fmt.Sprintf("%d", amount))
	commands := make([]*dog_pool.RedisBatchCommand, count)
	for i, key := range p.KEYS {
		commands[i] = dog_pool.MakeRedisBatchCommand(cmd)
		commands[i].WriteStringArg(key)
		commands[i].WriteArg(amount_bytes)
	}

	err := dog_pool.RedisBatchCommands(commands).ExecuteBatch(p.Redis)
	if err != nil {
		return nil, err
	}

	values := make([]int64, count)
	for i, key := range p.KEYS {
		ptr, err := toInt64Ptr(commands[i].Reply())
		switch {
		case nil != err:
			return nil, err
		case nil != ptr:
			p.cache.Set(key, ptr)
			values[i] = *ptr
		}
	}

	return values, nil
}

func (p *RedisMKeysCounterInt64) operationReplacesAmounts(amount int64) ([]int64, error) {
	p.cache.Reset()
	count := len(p.KEYS)

	amount_bytes := []byte(fmt.Sprintf("%d", amount))
	buffer := make([][]byte, len(p.KEYS)*2)[0:0]
	for _, key := range p.KEYS {
		buffer = append(buffer, []byte(key), amount_bytes)
	}

	reply := p.Redis.Cmd("MSET", buffer)
	if nil != reply.Err {
		return nil, reply.Err
	}

	values := make([]int64, count)
	for i, key := range p.KEYS {
		p.cache.Set(key, &amount)
		values[i] = amount
	}

	return values, nil
}
