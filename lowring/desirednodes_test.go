// Automatically generated with: gastly ../../holdme/internal/keysorderedbyvalues_test.go desirednodes_test.go lowring NumericKeyType=droptype:Node NumericValueType=droptype:int32 KeysOrderedByValues=desiredNodes Newd=newD Keys=byDesire Values=toDesire

package lowring

import (
	"math/rand"
	"testing"
)

func helperdesiredNodesInOrder(t *testing.T, x *desiredNodes) {
	f := true
	var pk Node
	var pv int32
	for i, k := range x.byDesire {
		v := x.toDesire[k]
		if f {
			f = false
		} else {
			if v > pv {
				t.Fatalf("%v:%v at index %d was greater than %v:%v at index %d", k, v, i, pk, pv, i-1)
			}
		}
		pk = k
		pv = v
	}
}

func helperdesiredNodesFind(t *testing.T, x *desiredNodes, k Node) int {
	for i, k2 := range x.byDesire {
		if k2 == k {
			return i
		}
	}
	t.Fatalf("could not find %v", k)
	return -1
}

func Test_desiredNodes_Add(t *testing.T) {
	var x *desiredNodes
	refresh := func() {
		x = &desiredNodes{}
		x.Add(0, 10)
		for k := Node(1); k < 6; k++ {
			x.Add(k, 0)
		}
		x.Add(6, -10)
		helperdesiredNodesFind(t, x, 0)
		for k := Node(1); k < 6; k++ {
			helperdesiredNodesFind(t, x, k)
		}
		helperdesiredNodesFind(t, x, 6)
	}
	refresh()
	x.RandIntn = nil
	x.Add(7, 0)
	helperdesiredNodesInOrder(t, x)
	i := helperdesiredNodesFind(t, x, 7)
	randIntn := rand.New(rand.NewSource(0)).Intn
	i2 := i
	for j := 0; j < 100; j++ {
		refresh()
		x.RandIntn = randIntn
		x.Add(7, 0)
		helperdesiredNodesInOrder(t, x)
		i2 = helperdesiredNodesFind(t, x, 7)
		if i2 != i {
			break
		}
	}
	if i2 == i {
		t.Fatal("expected positions to change", x)
	}
	randIntn = rand.New(rand.NewSource(1)).Intn
	i3 := i
	for j := 0; j < 100; j++ {
		refresh()
		x.RandIntn = randIntn
		x.Add(7, 0)
		helperdesiredNodesInOrder(t, x)
		i3 = helperdesiredNodesFind(t, x, 7)
		if i3 != i && i3 != i2 {
			break
		}
	}
	if i3 == i || i3 == i2 {
		t.Fatal("expected positions to change", x)
	}
	refresh()
	x.Add(7, 0)
	helperdesiredNodesInOrder(t, x)
	ln := len(x.byDesire)
	x.Add(7, 0)
	helperdesiredNodesInOrder(t, x)
	ln2 := len(x.byDesire)
	if ln != ln2 {
		t.Fatal("length changed", ln, ln2)
	}
}

func Test_desiredNodes_Move(t *testing.T) {
	x := &desiredNodes{}
	for k := Node(1); k < 10; k++ {
		x.Add(k, int32(k))
	}
	helperdesiredNodesInOrder(t, x)
	x.Move(3, 7)
	helperdesiredNodesInOrder(t, x)
	if x.toDesire[3] != 7 {
		t.Fatal("value was", x.toDesire[3])
	}
	x.Move(3, 7)
	helperdesiredNodesInOrder(t, x)
	if x.toDesire[3] != 7 {
		t.Fatal("value was", x.toDesire[3])
	}
	for k := Node(10); k < 15; k++ {
		x.Add(k, 5)
	}
	helperdesiredNodesInOrder(t, x)
	x.Move(7, 3)
	helperdesiredNodesInOrder(t, x)
	x.Move(7, 5)
	helperdesiredNodesInOrder(t, x)
	i := helperdesiredNodesFind(t, x, 7)
	x.RandIntn = rand.New(rand.NewSource(0)).Intn
	i2 := i
	for j := 0; j < 100; j++ {
		x.Move(7, 3)
		helperdesiredNodesInOrder(t, x)
		x.Move(7, 5)
		helperdesiredNodesInOrder(t, x)
		i2 = helperdesiredNodesFind(t, x, 7)
		if i2 != i {
			break
		}
	}
	if i2 == i {
		t.Fatal("expected positions to change", x)
	}
	x.RandIntn = rand.New(rand.NewSource(1)).Intn
	i3 := i
	for j := 0; j < 100; j++ {
		x.Move(7, 3)
		helperdesiredNodesInOrder(t, x)
		x.Move(7, 5)
		helperdesiredNodesInOrder(t, x)
		i3 = helperdesiredNodesFind(t, x, 7)
		if i3 != i && i3 != i2 {
			break
		}
	}
	if i3 == i || i3 == i2 {
		t.Fatal("expected positions to change", x)
	}
	x.RandIntn = rand.New(rand.NewSource(2)).Intn
	x.Move(7, 5)
	i = helperdesiredNodesFind(t, x, 7)
	i2 = i
	for j := 0; j < 100; j++ {
		x.Move(7, 5)
		helperdesiredNodesInOrder(t, x)
		i2 = helperdesiredNodesFind(t, x, 7)
		if i2 != i {
			break
		}
	}
	if i2 == i {
		t.Fatal("expected positions to change", x)
	}
}
