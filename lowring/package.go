//go:generate got valueorderedkeys.got      zzz_desirednodes.go         p=lowring new=newDesiredNodes T=desiredNodes K=byDesire k=Node V=toDesire v=int32
//go:generate got valueorderedkeys_test.got zzz_desirednodes_test.go    p=lowring new=newDesiredNodes T=desiredNodes K=byDesire k=Node V=toDesire v=int32

//go:generate got valueorderedkeys.got      zzz_desiredgroups.go        p=lowring new=newDesiredGroups T=desiredGroups K=byDesire k=int V=toDesire v=int32
//go:generate got valueorderedkeys_test.got zzz_desiredgroups_test.go   p=lowring new=newDesiredGroups T=desiredGroups K=byDesire k=int V=toDesire v=int32

package lowring
