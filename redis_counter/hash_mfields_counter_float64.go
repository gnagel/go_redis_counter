package redis_counter

import "fmt"
import "github.com/gnagel/dog_pool/dog_pool"
import . "github.com/gnagel/go_map_to_ptrs/map_to_ptrs"

type RedisHashMFieldsCounterFloat64 struct {
	Redis  *dog_pool.RedisConnection
	KEY    string
	FIELDS []string
	Cache  MapStringToFloat64Ptrs
}

// Make a new instance of RedisHashMFieldsCounterFloat64
func MakeRedisHashMFieldsCounterFloat64(redis *dog_pool.RedisConnection, key string, fields ...string) (*RedisHashMFieldsCounterFloat64, error) {
	switch {
	case nil == redis:
		return nil, fmt.Errorf("Nil redis connection")
	case len(key) == 0:
		return nil, fmt.Errorf("Empty redis key")
	case len(fields) == 0:
		return nil, fmt.Errorf("Empty redis fields")
	default:
		for i, field := range fields {
			if len(field) == 0 {
				return nil, fmt.Errorf("Empty redis field[%d]", i)
			}
		}

		return &RedisHashMFieldsCounterFloat64{
			Redis:  redis,
			KEY:    key,
			FIELDS: fields,
			Cache:  MakeMapStringToFloat64Ptrs(len(fields)),
		}, nil
	}
}

// Clear the contents of the cache
func (p *RedisHashMFieldsCounterFloat64) CacheReset() {
	for _, key := range p.FIELDS {
		p.Cache.Set(key, nil)
	}
}

// Format the values as a string; uses the cached "LastValues" field
func (p *RedisHashMFieldsCounterFloat64) String() string {
	return fmt.Sprintf("%s[%s]", p.KEY, p.Cache.String())
}

// Get the value of the counters; saves the counter to "LastValues"
func (p *RedisHashMFieldsCounterFloat64) MFloat64() ([]float64, error) {
	return p.operationReturnsAmounts("HMGET")
}

func (p *RedisHashMFieldsCounterFloat64) MExists() ([]bool, error) {
	p.CacheReset()

	return p.Redis.HashFieldsExist(p.KEY, p.FIELDS...)
}

func (p *RedisHashMFieldsCounterFloat64) MDelete() error {
	p.CacheReset()

	reply := p.Redis.Cmd("HDEL", p.KEY, p.FIELDS)
	return reply.Err
}

func (p *RedisHashMFieldsCounterFloat64) MGet() ([]float64, error) {
	return p.MFloat64()
}

func (p *RedisHashMFieldsCounterFloat64) MSet(amount float64) ([]float64, error) {
	return p.operationReplacesAmounts(amount)
}

func (p *RedisHashMFieldsCounterFloat64) MAdd(amount float64) ([]float64, error) {
	return p.operationModifiesAmounts("HINCRBY", amount)
}

func (p *RedisHashMFieldsCounterFloat64) MSub(amount float64) ([]float64, error) {
	return p.MAdd(-1 * amount)
}

func (p *RedisHashMFieldsCounterFloat64) MIncrement() ([]float64, error) {
	return p.MAdd(1)
}

func (p *RedisHashMFieldsCounterFloat64) MDecrement() ([]float64, error) {
	return p.MAdd(-1)
}

//
// Internal Helpers:
//

func (p *RedisHashMFieldsCounterFloat64) operationReturnsAmounts(cmd string) ([]float64, error) {
	p.CacheReset()

	reply := p.Redis.Cmd(cmd, p.KEY, p.FIELDS)
	switch {
	case nil != reply.Err:
		return nil, reply.Err
	default:
		count := len(p.FIELDS)
		values := make([]float64, count)
		for i, field := range p.FIELDS {
			ptr, err := toFloat64Ptr(reply.Elems[i])
			switch {
			case nil != err:
				return nil, err
			case nil != ptr:
				p.Cache.Set(field, ptr)
				values[i] = *ptr
			}
		}

		return values, nil
	}
}

func (p *RedisHashMFieldsCounterFloat64) operationReturnsAmount(cmd string) ([]float64, error) {
	p.CacheReset()

	count := len(p.FIELDS)
	commands := make([]*dog_pool.RedisBatchCommand, count)
	for i, field := range p.FIELDS {
		commands[i] = dog_pool.MakeRedisBatchCommand(cmd)
		commands[i].WriteStringArg(p.KEY)
		commands[i].WriteStringArg(field)
	}

	err := dog_pool.RedisBatchCommands(commands).ExecuteBatch(p.Redis)
	if err != nil {
		return nil, err
	}

	values := make([]float64, count)
	for i, field := range p.FIELDS {
		ptr, err := toFloat64Ptr(commands[i].Reply())
		switch {
		case nil != err:
			return nil, err
		case nil != ptr:
			p.Cache.Set(field, ptr)
			values[i] = *ptr
		}
	}

	return values, nil
}

func (p *RedisHashMFieldsCounterFloat64) operationModifiesAmounts(cmd string, amount float64) ([]float64, error) {
	p.CacheReset()

	count := len(p.FIELDS)
	amount_bytes := []byte(fmt.Sprintf("%d", amount))
	commands := make([]*dog_pool.RedisBatchCommand, count)
	for i, field := range p.FIELDS {
		commands[i] = dog_pool.MakeRedisBatchCommand(cmd)
		commands[i].WriteStringArg(p.KEY)
		commands[i].WriteStringArg(field)
		commands[i].WriteArg(amount_bytes)
	}

	err := dog_pool.RedisBatchCommands(commands).ExecuteBatch(p.Redis)
	if err != nil {
		return nil, err
	}

	values := make([]float64, count)
	for i, field := range p.FIELDS {
		ptr, err := toFloat64Ptr(commands[i].Reply())
		switch {
		case nil != err:
			return nil, err
		case nil != ptr:
			p.Cache.Set(field, ptr)
			values[i] = *ptr
		}
	}

	return values, nil
}

func (p *RedisHashMFieldsCounterFloat64) operationReplacesAmounts(amount float64) ([]float64, error) {
	p.CacheReset()

	count := len(p.FIELDS)
	amount_bytes := []byte(fmt.Sprintf("%d", amount))
	buffer := make([][]byte, len(p.FIELDS)*2)[0:0]
	for _, field := range p.FIELDS {
		buffer = append(buffer, []byte(field), amount_bytes)
	}

	reply := p.Redis.Cmd("HMSET", p.KEY, buffer)
	if nil != reply.Err {
		return nil, reply.Err
	}

	values := make([]float64, count)
	for i, field := range p.FIELDS {
		p.Cache.Set(field, &amount)
		values[i] = amount
	}

	return values, nil
}
