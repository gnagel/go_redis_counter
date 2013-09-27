package redis_counter

import "strconv"
import "github.com/fzzy/radix/redis"

func toFloat64Ptr(reply *redis.Reply) (*float64, error) {
	switch {
	case nil != reply.Err:
		return nil, reply.Err

	case redis.NilReply == reply.Type:
		return nil, nil

	case redis.IntegerReply == reply.Type:
		int_value, _ := reply.Int64()
		value := float64(int_value)
		return &value, nil

	default:
		str, str_err := reply.Str()
		if nil != str_err {
			return nil, str_err
		}

		value, err := strconv.ParseFloat(str, 64)
		if nil != err {
			return nil, err
		}

		return &value, nil
	}
}
