// gastly can be found at github.com/gholt/gastly

//go:generate gastly ../../holdme/internal/keysorderedbyvalues.go      desirednodes.go      lowring NumericKeyType=droptype:Node NumericValueType=droptype:int32 KeysOrderedByValues=desiredNodes Newd=newD Keys=byDesire Values=toDesire
//go:generate gastly ../../holdme/internal/keysorderedbyvalues_test.go desirednodes_test.go lowring NumericKeyType=droptype:Node NumericValueType=droptype:int32 KeysOrderedByValues=desiredNodes Newd=newD Keys=byDesire Values=toDesire

//go:generate gastly ../../holdme/internal/keysorderedbyvalues.go      desiredgroups.go      lowring NumericKeyType=droptype:int NumericValueType=droptype:int32 KeysOrderedByValues=desiredGroups Newd=newD Keys=byDesire Values=toDesire
//go:generate gastly ../../holdme/internal/keysorderedbyvalues_test.go desiredgroups_test.go lowring NumericKeyType=droptype:int NumericValueType=droptype:int32 KeysOrderedByValues=desiredGroups Newd=newD Keys=byDesire Values=toDesire

package lowring
