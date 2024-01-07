package btree

type BNode struct {
	data []byte // in-memory data, can be dumped to disk
}

// type of node
const (
	INTERNAL = 1
	LEAF     = 2
)

type BTree struct {
	root uint64 // disk page number
	// callbacks to manage disk page references
	get func(uint64) BNode // to derefrence pointer
	new func(BNode) uint64 // to allocate new page
	del func(uint64)       // to deallocate new page
}

const HEADER = 4

const BTREE_PAGE_SIZE = 4096

// size constraints
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE + 100
	if node1max > BTREE_PAGE_SIZE {
		panic("Exceeded page size!")
	}
}
