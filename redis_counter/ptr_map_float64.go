package redis_counter

import "fmt"
import "strings"

// Map of string keys to float64 pointers
type PtrMapFloat64 struct {
	initSize int
	m        map[string]*float64 "Generic key -> *float64 map"
}

func makePtrMapFloat64(size int) *PtrMapFloat64 {
	return &PtrMapFloat64{
		size,
		make(map[string]*float64, size),
	}
}

// Reset the map to nil
func (p *PtrMapFloat64) Reset() {
	p.m = make(map[string]*float64, p.initSize)
}

// How big is the map?
func (p *PtrMapFloat64) Len() int {
	return len(p.m)
}

// Get the value of the key
func (p *PtrMapFloat64) Value(key string) *float64 {
	ptr, ok := p.m[key]
	if !ok {
		return nil
	}
	return ptr
}

// Set the value of the key
func (p *PtrMapFloat64) Set(key string, value *float64) {
	p.m[key] = value
}

// Format the map as a string
func (p *PtrMapFloat64) String(keys []string) string {
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
				lines[i] = fmt.Sprintf("%s = %0.6f", key, *last_value)
			}
		}
	}

	return strings.Join(lines, ", ")
}
