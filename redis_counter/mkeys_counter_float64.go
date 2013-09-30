package redis_counter

import "fmt"
import "github.com/gnagel/dog_pool/dog_pool"
import . "github.com/gnagel/go_map_to_ptrs/map_to_ptrs"

type RedisMKeysCounterFloat64 struct {
	Redis *dog_pool.RedisConnection
	KEYS  []string
	Cache MapStringToFloat64Ptrs
}

// Make a new instance of RedisMKeysCounterFloat64
func MakeRedisMKeysCounterFloat64(redis *dog_pool.RedisConnection, keys ...string) (*RedisMKeysCounterFloat64, error) {
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

		p := &RedisMKeysCounterFloat64{
			Redis: redis,
			KEYS:  keys,
			Cache: MakeMapStringToFloat64Ptrs(len(keys)),
		}
		p.CacheReset()
		return p, nil
	}
}

// Clear the contents of the cache
func (p *RedisMKeysCounterFloat64) CacheReset() {
	for _, key := range p.KEYS {
		p.Cache.Set(key, nil)
	}
}

// Format the values as a string
func (p *RedisMKeysCounterFloat64) String() string {
	return p.Cache.String()
}

// Get the value of the counters; saves the counter to "LastValues"
func (p *RedisMKeysCounterFloat64) MFloat64() ([]float64, error) {
	return p.operationReturnsAmounts("MGET")
}

func (p *RedisMKeysCounterFloat64) MExists() ([]bool, error) {
	p.CacheReset()

	return p.Redis.KeysExist(p.KEYS...)
}

func (p *RedisMKeysCounterFloat64) MDelete() error {
	p.CacheReset()

	reply := p.Redis.Cmd("DEL", p.KEYS)
	return reply.Err
}

func (p *RedisMKeysCounterFloat64) MGet() ([]float64, error) {
	return p.MFloat64()
}

func (p *RedisMKeysCounterFloat64) MSet(amount float64) ([]float64, error) {
	return p.operationReplacesAmounts(amount)
}

func (p *RedisMKeysCounterFloat64) MAdd(amount float64) ([]float64, error) {
	return p.operationModifiesAmounts("INCRBY", amount)
}

func (p *RedisMKeysCounterFloat64) MSub(amount float64) ([]float64, error) {
	return p.operationModifiesAmounts("DECRBY", amount)
}

func (p *RedisMKeysCounterFloat64) MIncrement() ([]float64, error) {
	return p.operationReturnsAmount("INCR")
}

func (p *RedisMKeysCounterFloat64) MDecrement() ([]float64, error) {
	return p.operationReturnsAmount("DECR")
}

//
// Internal Helpers:
//

func (p *RedisMKeysCounterFloat64) operationReturnsAmounts(cmd string) ([]float64, error) {
	p.CacheReset()

	count := len(p.KEYS)

	reply := p.Redis.Cmd(cmd, p.KEYS)
	switch {
	case nil != reply.Err:
		return nil, reply.Err
	default:
		values := make([]float64, count)
		for i, key := range p.KEYS {
			ptr, err := toFloat64Ptr(reply.Elems[i])
			switch {
			case nil != err:
				return nil, err
			case nil != ptr:
				p.Cache.Set(key, ptr)
				values[i] = *ptr
			}
		}

		return values, nil
	}
}

func (p *RedisMKeysCounterFloat64) operationReturnsAmount(cmd string) ([]float64, error) {
	p.CacheReset()

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

	values := make([]float64, count)
	for i, key := range p.KEYS {
		ptr, err := toFloat64Ptr(commands[i].Reply())
		switch {
		case nil != err:
			return nil, err
		case nil != ptr:
			p.Cache.Set(key, ptr)
			values[i] = *ptr
		}
	}

	return values, nil
}

func (p *RedisMKeysCounterFloat64) operationModifiesAmounts(cmd string, amount float64) ([]float64, error) {
	p.CacheReset()

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

	values := make([]float64, count)
	for i, key := range p.KEYS {
		ptr, err := toFloat64Ptr(commands[i].Reply())
		switch {
		case nil != err:
			return nil, err
		case nil != ptr:
			p.Cache.Set(key, ptr)
			values[i] = *ptr
		}
	}

	return values, nil
}

func (p *RedisMKeysCounterFloat64) operationReplacesAmounts(amount float64) ([]float64, error) {
	p.CacheReset()

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

	values := make([]float64, count)
	for i, key := range p.KEYS {
		p.Cache.Set(key, &amount)
		values[i] = amount
	}

	return values, nil
}
