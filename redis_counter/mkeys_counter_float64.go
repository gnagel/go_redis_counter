package redis_counter

import "fmt"
import "github.com/gnagel/dog_pool/dog_pool"

type RedisMKeysCounterFloat64 struct {
	Redis dog_pool.RedisClientInterface
	KEYS  []string
	cache *PtrMapFloat64
}

// Make a new instance of RedisMKeysCounterFloat64
func MakeRedisMKeysCounterFloat64(redis dog_pool.RedisClientInterface, keys ...string) (*RedisMKeysCounterFloat64, error) {
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

		return &RedisMKeysCounterFloat64{redis, keys, makePtrMapFloat64(len(keys))}, nil
	}
}

func (p *RedisMKeysCounterFloat64) LastValue(key string) *float64 {
	return p.cache.Value(key)
}

// Format the values as a string; uses the cached "LastValues" field
func (p *RedisMKeysCounterFloat64) String() string {
	return p.cache.String(p.KEYS)
}

// Get the value of the counters; saves the counter to "LastValues"
func (p *RedisMKeysCounterFloat64) MFloat64() ([]float64, error) {
	return p.operationReturnsAmounts("MGET")
}

func (p *RedisMKeysCounterFloat64) MExists() ([]bool, error) {
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

func (p *RedisMKeysCounterFloat64) MDelete() error {
	p.cache.Reset()
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
	return p.operationModifiesAmounts("INCRBYFLOAT", amount)
}

func (p *RedisMKeysCounterFloat64) MSub(amount float64) ([]float64, error) {
	return p.MAdd(-1 * amount)
}

func (p *RedisMKeysCounterFloat64) MIncrement() ([]float64, error) {
	return p.MAdd(1.0)
}

func (p *RedisMKeysCounterFloat64) MDecrement() ([]float64, error) {
	return p.MSub(1.0)
}

//
// Internal Helpers:
//

func (p *RedisMKeysCounterFloat64) operationReturnsAmounts(cmd string) ([]float64, error) {
	p.cache.Reset()
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
				p.cache.Reset()
				return nil, err
			case nil != ptr:
				p.cache.Set(key, ptr)
				values[i] = *ptr
			}
		}

		return values, nil
	}
}

func (p *RedisMKeysCounterFloat64) operationReturnsAmount(cmd string) ([]float64, error) {
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

	values := make([]float64, count)
	for i, key := range p.KEYS {
		ptr, err := toFloat64Ptr(commands[i].Reply())
		switch {
		case nil != err:
			p.cache.Reset()
			return nil, err
		case nil != ptr:
			p.cache.Set(key, ptr)
			values[i] = *ptr
		}
	}

	return values, nil
}

func (p *RedisMKeysCounterFloat64) operationModifiesAmounts(cmd string, amount float64) ([]float64, error) {
	p.cache.Reset()

	count := len(p.KEYS)
	amount_bytes := []byte(fmt.Sprintf("%f", amount))
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
			p.cache.Reset()
			return nil, err
		case nil != ptr:
			p.cache.Set(key, ptr)
			values[i] = *ptr
		}
	}

	return values, nil
}

func (p *RedisMKeysCounterFloat64) operationReplacesAmounts(amount float64) ([]float64, error) {
	p.cache.Reset()
	count := len(p.KEYS)

	amount_bytes := []byte(fmt.Sprintf("%f", amount))
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
		p.cache.Set(key, &amount)
		values[i] = amount
	}

	return values, nil
}
