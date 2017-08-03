package ring

import "math"

// NodeIndexType is the type used when tracking node indexes.
//
// You can change this and NodeIndexNil and recompile to change the number of
// nodes allowed. This will impact memory usage, e.g. the Ring is a
// [][]NodeIndexType and so would use:
// replicas * partitions * unsafe.Sizeof(NodeIndexType)
type NodeIndexType uint16

// NodeIndexNil is the value used to represent no node assignment.
//
// It should be set to the maximum NodeIndexType value.
const NodeIndexNil NodeIndexType = math.MaxUint16

// LastMovedType is the type used to track when replicas were last moved.
//
// You can change this and LastMovedMax and recompile to change the time range
// (in co-ordination with LastMovedUnit) measurable.
// This will impact memory usage, the Builder has a LastMoved field and is a
// [][]LastMovedType and so would use:
// replicas * partitions * unsafe.Sizeof(LastMovedType)
type LastMovedType uint16

// LastMovedMax is the maximum value of a LastMovedType.
const LastMovedMax LastMovedType = math.MaxUint16
