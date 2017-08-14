// Copied from github.com/gholt/holdme

package lowring

import "fmt"

// desiredNodes keeps byDesire in order by toDesire[byDesire[i]] value.
//
// It assumes a relatively static set in byDesire that you want to keep sorted as
// their values in toDesire change. You first add all the keys using Add and then
// use Move to change the values. As you do so, byDesire will be kept in order.
//
// Note that the values of the keys are simply stored in a slice and not a map,
// so wildly varying min/max keys will use a lot of memory. It should be easy
// to modify to use a map if desired though; for my use, a slice was fine and
// much faster than a map.
//
// The RandIntn function may be set to randomize key locations whose values are
// the same. Even if RandIntn is nil, the order of keys whose values are the
// same is not guaranteed to be in any particular order (think like Go's map
// key order, no guarantees).
//
// I know it's a weird "key/value randomization bag" or something and probably
// not generally useful, but I had to create it for my
// github.com/gholt/ring/lowring package where I needed to sort nodes and
// groups by their desires, so here it is.
//
// This code will copy memory quite a bit and can probably be improved with
// scanning for value runs and swapping. Of course, if there are few runs,
// swapping "by hand" will probably be slower than just calling copy. Not sure
// yet, just haven't gotten that far. Oh, and another note if you (or future
// me) decides to try this optimization, you'll have to swap before applying
// the RandIntn, and then use that to swap a second time.
type desiredNodes struct {
	byDesire []Node
	toDesire []int32
	RandIntn func(int) int
}

func newDesiredNodes(cap int, randIntn func(int) int) *desiredNodes {
	return &desiredNodes{
		byDesire: make([]Node, 0, cap),
		toDesire: make([]int32, 0, cap),
		RandIntn: randIntn,
	}
}

func (x *desiredNodes) Add(key Node, value int32) {
	ln := len(x.toDesire)
	if int(key) < ln {
		x.Move(key, value)
		return
	}
	if int(key) == ln {
		x.toDesire = append(x.toDesire, value)
	} else {
		x.toDesire = append(x.toDesire, make([]int32, int(key)-ln+1)...)
		x.toDesire[key] = value
	}
	right := 0
	hi := len(x.byDesire)
	for right < hi {
		mid := (right + hi) / 2
		if value > x.toDesire[x.byDesire[mid]] {
			hi = mid
		} else {
			right = mid + 1
		}
	}
	if x.RandIntn != nil {
		left := 0
		hi = len(x.byDesire)
		for left < hi {
			mid := (left + hi) / 2
			if x.toDesire[x.byDesire[mid]] > value {
				left = mid + 1
			} else {
				hi = mid
			}
		}
		if right-left > 2 {
			right = right - x.RandIntn(right-left)
		}
	}
	x.byDesire = append(x.byDesire, 0)
	copy(x.byDesire[right+1:], x.byDesire[right:])
	x.byDesire[right] = key
}

func (x *desiredNodes) Move(key Node, value int32) {
	var oldPosition int
	oldValue := x.toDesire[key]
	right := 0
	hi := len(x.byDesire)
	for right < hi {
		mid := (right + hi) / 2
		if oldValue > x.toDesire[x.byDesire[mid]] {
			hi = mid
		} else {
			right = mid + 1
		}
	}
	right--
	if x.byDesire[right] == key {
		oldPosition = right
	} else {
		left := 0
		hi = len(x.byDesire)
		for left < hi {
			mid := (left + hi) / 2
			if x.toDesire[x.byDesire[mid]] > oldValue {
				left = mid + 1
			} else {
				hi = mid
			}
		}
		if x.byDesire[left] == key {
			oldPosition = left
		} else {
			for oldPosition = left + 1; x.byDesire[oldPosition] != key && oldPosition < right; oldPosition++ {
			}
		}
	}
	right = 0
	hi = len(x.byDesire)
	for right < hi {
		mid := (right + hi) / 2
		if value > x.toDesire[x.byDesire[mid]] {
			hi = mid
		} else {
			right = mid + 1
		}
	}
	if x.RandIntn != nil {
		left := 0
		hi = len(x.byDesire)
		for left < hi {
			mid := (left + hi) / 2
			if x.toDesire[x.byDesire[mid]] > value {
				left = mid + 1
			} else {
				hi = mid
			}
		}
		if right-left > 2 {
			right = right - x.RandIntn(right-left)
		}
	}
	if right < oldPosition {
		copy(x.byDesire[right+1:], x.byDesire[right:oldPosition])
		x.byDesire[right] = key
	} else if oldPosition < right {
		copy(x.byDesire[oldPosition:], x.byDesire[oldPosition+1:right])
		x.byDesire[right-1] = key
	}
	x.toDesire[key] = value
}

func (x *desiredNodes) String() string {
	s := "["
	for i, key := range x.byDesire {
		if i == 0 {
			s += fmt.Sprintf("%d:%d", key, x.toDesire[key])
		} else {
			s += fmt.Sprintf(" %d:%d", key, x.toDesire[key])
		}
	}
	return s + "]"
}
