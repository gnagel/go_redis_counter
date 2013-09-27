package redis_counter

import "testing"
import "github.com/orfjackal/gospec/src/gospec"

func TestPtrMapFloat64Specs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(PtrMapFloat64Specs)
	gospec.MainGoTest(r, t)
}

func PtrMapFloat64Specs(c gospec.Context) {

	c.Specify("[PtrMapFloat64][Reset]", func() {
		value := &PtrMapFloat64{}
		value.m = map[string]*float64{
			"Bob":    nil,
			"George": nil,
		}

		c.Expect(value.m, gospec.Satisfies, nil != value.m)
		c.Expect(value.m, gospec.Satisfies, 2 == len(value.m))

		value.Reset()
		c.Expect(value.m, gospec.Satisfies, 0 == len(value.m))
	})

	c.Specify("[PtrMapFloat64][Len]", func() {
		value := &PtrMapFloat64{}
		value.m = map[string]*float64{
			"Bob":    nil,
			"George": nil,
		}

		c.Expect(value.Len(), gospec.Equals, 2)
	})

	c.Specify("[PtrMapFloat64][Value]", func() {
		count := float64(123)
		value := &PtrMapFloat64{}
		value.m = map[string]*float64{
			"Bob":    &count,
			"George": nil,
		}

		c.Expect(value.Value("Bob"), gospec.Equals, &count)
		c.Expect(*value.Value("Bob"), gospec.Equals, float64(123))
		c.Expect(value.Value("George"), gospec.Satisfies, nil == value.Value("George"))
		c.Expect(value.Value("Cache Miss"), gospec.Satisfies, nil == value.Value("Cache Miss"))
	})

	c.Specify("[PtrMapFloat64][String]", func() {
		count := float64(123)
		value := &PtrMapFloat64{}
		value.m = map[string]*float64{
			"Bob":    &count,
			"George": nil,
		}

		c.Expect(value.String([]string{"Bob", "George"}), gospec.Equals, "Bob = 123.000000, George = NaN")
	})

}
