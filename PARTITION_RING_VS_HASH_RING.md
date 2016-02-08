Partition Ring vs. Hash Ring

While not the perfect title, hopefully it conveys that *this* ring library
isn't exactly like *that* ring library. Many consistent hashing rings use a
sorted list of hashes paired with a map of those hashes back to their
corresponding nodes. This library uses a list of partitions of the hash space,
and it is those partitions that point to their corresponding nodes. The
difference allows for more precise assignments and faster lookups, but at a
cost of upfront build and update speeds, and perhaps memory usage.

I won't try to fully explain consistent hashing rings on this page, perhaps I
will elsewhere. On this page I'm going to assume you already know what
consistent hashing is.

More common rings keep a sorted list of virtual nodes, where virtual nodes are
just hashes based on the actual node. Usually the sorted list just contains the
hashes themselves, and a companion map is used to translate from the virtual
node hash back to its actual node. Selecting a responsible node for an item
involves generating a hash for the item and using that hash to find the next
virtual node in the sorted list of hashes, which then gives a reference to the
actual node via the companion map.

At its most basic, this algorithm will give very unbalanced rings. Imagine a
two node ring and selecting one virtual node per actual node, likely one node
will be responsible for much more than 50% of the work. A solution to this
problem is to create more virtual nodes, the more made, the more balanced the
ring becomes. Of course this is at the expense of memory and time to compute
the virtual node hashes and keep them in sorted order.

If instead I use a partition table, this simple two node ring can be perfectly
balanced. I make a two element list, each element representing half the hash
space, and I have each element point to one of the nodes. Selecting the
responsible node for some work still involves generating a hash for that work,
but then just one bit of that hash is used to select which of the two nodes to
use.

Generating this ring can use less memory and be much quicker (but isn't always,
see below) as lots of virtual hashes don't have to be generated, they don't
have to be kept in sorted order, and there is no map back to actual nodes to
maintain. Lookups for work can be faster as well: instead of a binary search
for a virtual node and then a map lookup to actual node, it can perform a
bitmask to get a partition index, use that partition index to lookup the node
index, and then use that node index to get the actual node from the list of
nodes.

There are also differences when the items being balanced need a multiple of
replicas [see footnote 1], such as when storing data and three copies are
needed for durability or availability. With the more common hash ring, I would
walk the sorted list from the point where I looked up the initial virtual node
and start looking for additional distinct nodes. With the partition ring the
replica assignments are done upfront, so I would create a partition ring for
each replica and specifically assign distinct nodes for each partition across
these replica rings. The benefit of the upfront work grows as the distinct node
selection grows more complex, such as multiple tiers of node separations
(network switch separation, power separations, data center or region
separations, etc.)

Keeping multiple rings in memory may seem like the partition ring would use
more memory than the hash ring, and I have no doubt it could in many cases. But
I think in most cases the partition ring would actually use less memory.
Consider that simple example of a two node, three replica use case. Let's
assume I decide ten virtual nodes per actual node are fine; probably too small,
but let's be generous. So the hash ring needs a sorted list of 20 virtual nodes
and map of size 20 to map virtual nodes back to actual nodes. The partition
ring needs three two-element lists and the list of nodes. Obviously this is an
oversimplified example, and I'd need to do a lot more real world examples of
both algorithms for comparision. For me, I'm comfortable with the memory usage
of the partition ring for the speed and control I gain using it.

One of the controls gained with the multiple partition ring concept is the
precise reassignment of items when the ring is modified. With the partition
ring, I can ensure only one (or two, etc.) of the replicas of each partition is
reassigned during a given time span. This gives the cluster time to move data
around and still have the other copies in place. With the hash ring, there is
no way that I know of to guarantee this.

Another consideration is that most rings have a concept of node weights, or
capacities. For a hash ring, nodes with greater weights have more virtual
nodes. For a partition ring, more partitions are specifically assigned to nodes
with greater weights. Both rings are impacted by the difference in the minimum
and maximum weights. With the hash ring, if the lightest node has 1 virtual
node and the heaviest node has 100 times more capacity, the heavy node will
need at least 100 times more virtual nodes. With the partition ring and the
same nodes, it will need at least 101 partitions. The hash ring will likely
need many more virtual nodes than the 101 to approach more perfect balances,
and the partition ring will likely use a power of 2 for the partition count
(easier to double and halve during size changes), lowering its perfect
balances.

In summary, I'd say the tradeoff is that much more must be done creating and
updating a partition ring than creating and updating a hash ring; but
assignments are under much more control and they are usually done far less
often than routing items, which is far more efficient with a partition ring.
Both can achieve better balancing by using more memory, but I'd need to do more
testing to truly determine which would use less for the same impact.

_[footnote 1] Many papers and talks about hash rings use the term "replica"
when referring to what I call a "virtual node" here. I use the term "replica"
when referring to a copy of the actual data stored (or work assignments,
etc.)._
