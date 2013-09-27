package redis_counter

import "fmt"
import "strings"

// Map of string keys to int64 pointers
type PtrMapInt64 struct {
	initSize int
	m        map[string]*int64 "Generic key -> *int64 map"
}

func makePtrMapInt64(size int) *PtrMapInt64 {
	return &PtrMapInt64{
		size,
		make(map[string]*int64, size),
	}
}

// Reset the map to nil
func (p *PtrMapInt64) Reset() {
	p.m = make(map[string]*int64, p.initSize)
}

// How big is the map?
func (p *PtrMapInt64) Len() int {
	return len(p.m)
}

// Get the value of the key
func (p *PtrMapInt64) Value(key string) *int64 {
	ptr, ok := p.m[key]
	if !ok {
		return nil
	}
	return ptr
}

// Set the value of the key
func (p *PtrMapInt64) Set(key string, value *int64) {
	p.m[key] = value
}

// Format the map as a string
func (p *PtrMapInt64) String(keys []string) string {
	lines := make([]string, len(keys))
	switch p.Len() {
	case 0:
		for i, key := range keys {
			lines[i] = fmt.Sprintf("%s = NaN", key)
		}
	default:
		for i, key := range keys {
			switch last_value := p.Value(key); {
			case nil == last_value:
				lines[i] = fmt.Sprintf("%s = NaN", key)
			default:
				lines[i] = fmt.Sprintf("%s = %d", key, *last_value)
			}
		}
	}

	return strings.Join(lines, ", ")
}
