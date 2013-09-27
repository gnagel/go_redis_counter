package redis_counter

import "github.com/fzzy/radix/redis"

func toInt64Ptr(reply *redis.Reply) (*int64, error) {
	switch {
	case nil != reply.Err:
		return nil, reply.Err

	case redis.NilReply == reply.Type:
		return nil, nil

	default:
		value, err := reply.Int64()
		if nil != err {
			return nil, err
		}

		return &value, nil
	}
}
