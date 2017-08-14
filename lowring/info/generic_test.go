// These are just some generic tests for different possible optimization
// considerations.
//
// As always, clearer code is usually more important than faster code, but
// sometimes we just can't help ourselves, right?
//
// I ran all these with -benchtime 30s at least. Of course, various
// architectures and Go versions (1.8.3 as I write this) probably vary as well,
// so run the tests yourself if you're curious. I tried to add "meaningless"
// code that would hopefully avoid any unfair compiler optimizations, but I may
// have missed some.
//
// BenchmarkA is just testing different ways of calling tiny functions, they
// take/use two variables and return a bool as a one-liner. Closures seem to
// suffer in this case, which might actually be where you'd think to use a
// quick closure instead of creating a "full blown" function. I found it a bit
// curious that MethodQV1b was faster than MethodQV1 by a smidge; nothing worth
// switching over, but curious.
//
//      BenchmarkA_FunctionQV1  0.28 ns/op
//      BenchmarkA_MethodQV1b   0.28 ns/op
//      BenchmarkA_MethodQV1    0.30 ns/op
//      BenchmarkA_ClosureQV1   1.97 ns/op
//
// BenchmarkB has a "for" loop to do a lot more work per call. Now closures
// really shine; the next best takes twice as long.
//
//      BenchmarkB_ClosureV1    2828 ns/op
//      BenchmarkB_FunctionV1   5628 ns/op
//      BenchmarkB_MethodV1b    5633 ns/op
//      BenchmarkB_MethodV1     5642 ns/op
//
// BenchmarkC adds 3 more variables, closures are still shining, but a method
// on a struct is pretty much the same. To continue the curious comment from
// above, now the "b" form of the method is measurably slower; it seems caching
// instance variables as local variables probably isn't worth it unless the
// code is just much more readable that way. Functions lose out here, which is
// probably expected since those extra variables are getting shoved onto the
// stack.
//
//      BenchmarkC_ClosureV4    16933 ns/op
//      BenchmarkC_MethodV4     16985 ns/op
//      BenchmarkC_MethodV4b    19750 ns/op
//      BenchmarkC_FunctionV4   22542 ns/op
//
// BenchmarkD compares nested field access. I was curious if all those times I
// was just dereferencing outside a loop was really helping. Turns out, not so
// much.
//
//      BenchmarkD_Method       7510 ns/op
//      BenchmarkD_Methodb      8427 ns/op
//
// BenchmarkE, F, and G compare using range versus grabbing the len and
// iterating yourself. E is a 1,000 elements, F is 10,000,000 elements, G is
// the same as F but puts the len outside the test loop (for fixed size
// slices). Not much difference with small slices (E) and a pretty big
// difference with huge slices (F). G really didn't have much effect in this
// test, but it only ran 10,000 times in the -benchtime 30s so... I made an H
// test similar to G (moves len outside the test loop) to see how it compares
// to the BenchmarkE_notrange. No real difference and this ran 100,000,000
// times. I don't really know what optimization is going on there, because
// BenchmarkH_notrangeb is definitely slower (as expected, it calls len a lot
// more).
//
//      BenchmarkE_notrange         431 ns/op
//      BenchmarkE_range            433 ns/op
//      BenchmarkF_notrange     4487955 ns/op
//      BenchmarkF_range        5812042 ns/op
//      BenchmarkG_notrange     4503606 ns/op
//      BenchmarkH_notrange         434 ns/op
//      BenchmarkH_notrangeb        570 ns/op

package info

import "testing"

func functionQV1(v []int, i int) bool {
	return len(v) == i
}

func BenchmarkA_FunctionQV1(b *testing.B) {
	v := []int{}
	for i := 0; i < b.N; i++ {
		functionQV1(v, i)
	}
}

func BenchmarkA_ClosureQV1(b *testing.B) {
	v := []int{}
	closureQV1 := func(i int) bool {
		return len(v) == i
	}
	for i := 0; i < b.N; i++ {
		closureQV1(i)
	}
}

type objectQV1 struct {
	v []int
}

func (instanceQV1 *objectQV1) methodQV1(i int) bool {
	return len(instanceQV1.v) == i
}

func BenchmarkA_MethodQV1(b *testing.B) {
	instanceQV1 := &objectQV1{}
	for i := 0; i < b.N; i++ {
		instanceQV1.methodQV1(i)
	}
}

func (instanceQV1 *objectQV1) methodQV1b(i int) bool {
	v := instanceQV1.v
	return len(v) == i
}

func BenchmarkA_MethodQV1b(b *testing.B) {
	instanceQV1 := &objectQV1{}
	for i := 0; i < b.N; i++ {
		instanceQV1.methodQV1b(i)
	}
}

func functionV1(v []int, i int) bool {
	for j := 0; j <= 10000; j++ {
		if len(v) == i {
			return true
		}
		if len(v) == 123 {
			v = nil
		}
	}
	return false
}

func BenchmarkB_FunctionV1(b *testing.B) {
	v := []int{}
	for i := 0; i < b.N; i++ {
		functionV1(v, i)
	}
}

func BenchmarkB_ClosureV1(b *testing.B) {
	v := []int{}
	closureV1 := func(i int) bool {
		for j := 0; j <= 10000; j++ {
			if len(v) == i {
				return true
			}
		}
		return false
	}
	for i := 0; i < b.N; i++ {
		closureV1(i)
	}
}

type objectV1 struct {
	v []int
}

func (instanceV1 *objectV1) methodV1(i int) bool {
	for j := 0; j <= 10000; j++ {
		if len(instanceV1.v) == i {
			return true
		}
		if len(instanceV1.v) == 123 {
			instanceV1.v = nil
		}
	}
	return false
}

func BenchmarkB_MethodV1(b *testing.B) {
	instanceV1 := &objectV1{}
	for i := 0; i < b.N; i++ {
		instanceV1.methodV1(i)
	}
}

func (instanceV1 *objectV1) methodV1b(i int) bool {
	v := instanceV1.v
	for j := 0; j <= 10000; j++ {
		if len(v) == i {
			return true
		}
		if len(v) == 123 {
			instanceV1.v = nil
			v = nil
		}
	}
	return false
}

func BenchmarkB_MethodV1b(b *testing.B) {
	instanceV1 := &objectV1{}
	for i := 0; i < b.N; i++ {
		instanceV1.methodV1b(i)
	}
}

func functionV4(v []int, v2 []byte, v3 []float64, v4 []string, i int) bool {
	for j := 0; j <= 10000; j++ {
		if len(v) == i || len(v2) == i || len(v3) == i || len(v4) == i {
			return true
		}
		if len(v) == 123 {
			v = nil
		}
		if len(v2) == 123 {
			v2 = nil
		}
		if len(v3) == 123 {
			v3 = nil
		}
		if len(v4) == 123 {
			v4 = nil
		}
	}
	return false
}

func BenchmarkC_FunctionV4(b *testing.B) {
	v := []int{}
	v2 := []byte{}
	v3 := []float64{}
	v4 := []string{}
	for i := 0; i < b.N; i++ {
		functionV4(v, v2, v3, v4, i)
	}
}

func BenchmarkC_ClosureV4(b *testing.B) {
	v := []int{}
	v2 := []byte{}
	v3 := []float64{}
	v4 := []string{}
	closureV4 := func(i int) bool {
		for j := 0; j <= 10000; j++ {
			if len(v) == i || len(v2) == i || len(v3) == i || len(v4) == i {
				return true
			}
			if len(v) == 123 {
				v = nil
			}
			if len(v2) == 123 {
				v2 = nil
			}
			if len(v3) == 123 {
				v3 = nil
			}
			if len(v4) == 123 {
				v4 = nil
			}
		}
		return false
	}
	for i := 0; i < b.N; i++ {
		closureV4(i)
	}
}

type objectV4 struct {
	v  []int
	v2 []byte
	v3 []float64
	v4 []string
}

func (instanceV4 *objectV4) methodV4(i int) bool {
	for j := 0; j <= 10000; j++ {
		if len(instanceV4.v) == i || len(instanceV4.v2) == i || len(instanceV4.v3) == i || len(instanceV4.v4) == i {
			return true
		}
		if len(instanceV4.v) == 123 {
			instanceV4.v = nil
		}
		if len(instanceV4.v2) == 123 {
			instanceV4.v2 = nil
		}
		if len(instanceV4.v3) == 123 {
			instanceV4.v3 = nil
		}
		if len(instanceV4.v4) == 123 {
			instanceV4.v4 = nil
		}
	}
	return false
}

func BenchmarkC_MethodV4(b *testing.B) {
	instanceV4 := &objectV4{}
	for i := 0; i < b.N; i++ {
		instanceV4.methodV4(i)
	}
}

func (instanceV4 *objectV4) methodV4b(i int) bool {
	v := instanceV4.v
	v2 := instanceV4.v2
	v3 := instanceV4.v3
	v4 := instanceV4.v4
	for j := 0; j <= 10000; j++ {
		if len(v) == i || len(v2) == i || len(v3) == i || len(v4) == i {
			return true
		}
		if len(v) == 123 {
			instanceV4.v = nil
			v = nil
		}
		if len(v2) == 123 {
			instanceV4.v2 = nil
			v2 = nil
		}
		if len(v3) == 123 {
			instanceV4.v3 = nil
			v3 = nil
		}
		if len(v4) == 123 {
			instanceV4.v4 = nil
			v4 = nil
		}
	}
	return false
}

func BenchmarkC_MethodV4b(b *testing.B) {
	instanceV4 := &objectV4{}
	for i := 0; i < b.N; i++ {
		instanceV4.methodV4b(i)
	}
}

type objectD struct {
	a *objectDa
}

type objectDa struct {
	v []int
}

func (instanceD *objectD) method(i int) bool {
	for j := 0; j <= 10000; j++ {
		if len(instanceD.a.v) == i {
			return true
		}
		if len(instanceD.a.v) == 123 {
			instanceD.a.v = append(instanceD.a.v, 123)
		}
	}
	return false
}

func BenchmarkD_Method(b *testing.B) {
	instanceD := &objectD{&objectDa{}}
	for i := 0; i < b.N; i++ {
		instanceD.method(i)
	}
}

func (instanceD *objectD) methodb(i int) bool {
	v := instanceD.a.v
	for j := 0; j <= 10000; j++ {
		if len(v) == i {
			return true
		}
		if len(v) == 123 {
			v = append(v, 123)
			instanceD.a.v = v
		}
	}
	return false
}

func BenchmarkD_Methodb(b *testing.B) {
	instanceD := &objectD{&objectDa{}}
	for i := 0; i < b.N; i++ {
		instanceD.methodb(i)
	}
}

var benchmarkE = make([]int, 1000)

func BenchmarkE_range(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for i, v := range benchmarkE {
			if i == -1 || v != 0 {
				return
			}
		}
	}
}

func BenchmarkE_notrange(b *testing.B) {
	for i := 0; i < b.N; i++ {
		c := len(benchmarkE)
		for i := 0; i < c; i++ {
			v := benchmarkE[i]
			if i == -1 || v != 0 {
				return
			}
		}
	}
}

var benchmarkF = make([]int, 10000000)

func BenchmarkF_range(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for i, v := range benchmarkF {
			if i == -1 || v != 0 {
				return
			}
		}
	}
}

func BenchmarkF_notrange(b *testing.B) {
	for i := 0; i < b.N; i++ {
		c := len(benchmarkF)
		for i := 0; i < c; i++ {
			v := benchmarkF[i]
			if i == -1 || v != 0 {
				return
			}
		}
	}
}

func BenchmarkG_notrange(b *testing.B) {
	c := len(benchmarkF)
	for i := 0; i < b.N; i++ {
		for i := 0; i < c; i++ {
			v := benchmarkF[i]
			if i == -1 || v != 0 {
				return
			}
		}
	}
}

func BenchmarkH_notrange(b *testing.B) {
	c := len(benchmarkE)
	for i := 0; i < b.N; i++ {
		for i := 0; i < c; i++ {
			v := benchmarkE[i]
			if i == -1 || v != 0 {
				return
			}
		}
	}
}

func BenchmarkH_notrangeb(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for i := 0; i < len(benchmarkE); i++ {
			v := benchmarkE[i]
			if i == -1 || v != 0 {
				return
			}
		}
	}
}
