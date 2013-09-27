package redis_counter

import "fmt"
import "github.com/gnagel/dog_pool/dog_pool"

type RedisHashMFieldsCounterInt64 struct {
	Redis  dog_pool.RedisClientInterface
	KEY    string
	FIELDS []string
	cache  *PtrMapInt64
}

// Make a new instance of RedisHashMFieldsCounterInt64
func MakeRedisHashMFieldsCounterInt64(redis dog_pool.RedisClientInterface, key string, fields ...string) (*RedisHashMFieldsCounterInt64, error) {
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

		return &RedisHashMFieldsCounterInt64{
			Redis:  redis,
			KEY:    key,
			FIELDS: fields,
			cache:  makePtrMapInt64(len(fields)),
		}, nil
	}
}

func (p *RedisHashMFieldsCounterInt64) LastValue(field string) *int64 {
	return p.cache.Value(field)
}

// Format the values as a string; uses the cached "LastValues" field
func (p *RedisHashMFieldsCounterInt64) String() string {
	return fmt.Sprintf("%s[%s]", p.KEY, p.cache.String(p.FIELDS))
}

// Get the value of the counters; saves the counter to "LastValues"
func (p *RedisHashMFieldsCounterInt64) MInt64() ([]int64, error) {
	return p.operationReturnsAmounts("HMGET")
}

func (p *RedisHashMFieldsCounterInt64) MExists() ([]bool, error) {
	p.cache.Reset()

	count := len(p.FIELDS)
	commands := make([]*dog_pool.RedisBatchCommand, count)
	for i, field := range p.FIELDS {
		commands[i] = dog_pool.MakeRedisBatchCommandHashExists(p.KEY, field)
	}

	err := dog_pool.RedisBatchCommands(commands).ExecuteBatch(p.Redis)
	if err != nil {
		return nil, err
	}

	exists := make([]bool, count)
	for i := range p.FIELDS {
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

func (p *RedisHashMFieldsCounterInt64) MDelete() error {
	p.cache.Reset()
	reply := p.Redis.Cmd("HDEL", p.KEY, p.FIELDS)
	return reply.Err
}

func (p *RedisHashMFieldsCounterInt64) MGet() ([]int64, error) {
	return p.MInt64()
}

func (p *RedisHashMFieldsCounterInt64) MSet(amount int64) ([]int64, error) {
	return p.operationReplacesAmounts(amount)
}

func (p *RedisHashMFieldsCounterInt64) MAdd(amount int64) ([]int64, error) {
	return p.operationModifiesAmounts("HINCRBY", amount)
}

func (p *RedisHashMFieldsCounterInt64) MSub(amount int64) ([]int64, error) {
	return p.MAdd(-1 * amount)
}

func (p *RedisHashMFieldsCounterInt64) MIncrement() ([]int64, error) {
	return p.MAdd(1)
}

func (p *RedisHashMFieldsCounterInt64) MDecrement() ([]int64, error) {
	return p.MAdd(-1)
}

//
// Internal Helpers:
//

func (p *RedisHashMFieldsCounterInt64) operationReturnsAmounts(cmd string) ([]int64, error) {
	p.cache.Reset()

	reply := p.Redis.Cmd(cmd, p.KEY, p.FIELDS)
	switch {
	case nil != reply.Err:
		return nil, reply.Err
	default:
		count := len(p.FIELDS)
		values := make([]int64, count)
		for i, field := range p.FIELDS {
			ptr, err := toInt64Ptr(reply.Elems[i])
			switch {
			case nil != err:
				return nil, err
			case nil != ptr:
				p.cache.Set(field, ptr)
				values[i] = *ptr
			}
		}

		return values, nil
	}
}

func (p *RedisHashMFieldsCounterInt64) operationReturnsAmount(cmd string) ([]int64, error) {
	p.cache.Reset()

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

	values := make([]int64, count)
	for i, field := range p.FIELDS {
		ptr, err := toInt64Ptr(commands[i].Reply())
		switch {
		case nil != err:
			return nil, err
		case nil != ptr:
			p.cache.Set(field, ptr)
			values[i] = *ptr
		}
	}

	return values, nil
}

func (p *RedisHashMFieldsCounterInt64) operationModifiesAmounts(cmd string, amount int64) ([]int64, error) {
	p.cache.Reset()

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

	values := make([]int64, count)
	for i, field := range p.FIELDS {
		ptr, err := toInt64Ptr(commands[i].Reply())
		switch {
		case nil != err:
			return nil, err
		case nil != ptr:
			p.cache.Set(field, ptr)
			values[i] = *ptr
		}
	}

	return values, nil
}

func (p *RedisHashMFieldsCounterInt64) operationReplacesAmounts(amount int64) ([]int64, error) {
	p.cache.Reset()

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

	values := make([]int64, count)
	for i, field := range p.FIELDS {
		p.cache.Set(field, &amount)
		values[i] = amount
	}

	return values, nil
}
