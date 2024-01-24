package btree

import (
	"encoding/binary"
	"unsafe"
)

type BNode struct {
	data []byte // in-memory data, can be dumped to disk
}

// type of node
const (
	BNODE_NODE = 1 // internal nodes without values
	BNODE_LEAF = 2 // leaf nodes with values
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
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	if node1max > BTREE_PAGE_SIZE {
		panic("Exceeded page size!")
	}
}

// header functions
// returns node type
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

// returns number of keys
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

// sets header with first two bytes for node type, second two for number of keys
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointer functions
// returns pointer to child node at given index
func (node BNode) getPtr(idx uint16) uint64 {
	if idx >= node.nkeys() {
		panic("Index out of bounds!")
	}
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}

// update child node pointer
func (node BNode) setPtr(idx uint16, val uint64) {
	if idx >= node.nkeys() {
		panic("Index out of bounds!")
	}
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// offset functions
// returns the value of the offset i.e. the location of the kv-pair at given index
func offsetPos(node BNode, idx uint16) uint16 {
	if idx < 1 || idx > node.nkeys() {
		panic("Index out of bounds!")
	}
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

// returns offset of kv-pair at given index
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}

// updates offset for kv-pair at given index
func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

// key-value pair functions
// returns position/byte-offset of kv-pair at idx
func (node BNode) kvPos(idx uint16) uint16 {
	if idx > node.nkeys() {
		panic("Index out of bounds!")
	}
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

// returns key of kv-pair at idx from data array
func (node BNode) getKey(idx uint16) []byte {
	if idx >= node.nkeys() {
		panic("Index out of bounds!")
	}
	pos := node.kvPos(idx)                              // byte position of kv-pair
	klen := binary.LittleEndian.Uint16(node.data[pos:]) // 2 bytes that represent the key length
	return node.data[pos+4:][:klen]                     // skip klen, vlen, return key
}

// returns value of kv-pair at idx from data array
func (node BNode) getVal(idx uint16) []byte {
	if idx >= node.nkeys() {
		panic("Index out of bounds!")
	}
	pos := node.kvPos(idx) // byte position of kv-pair
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

// return node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// tree container struct
type C struct {
	tree  BTree
	ref   map[string]string // reference map to record each b-tree update
	pages map[uint64]BNode  // hashmap to hold pages in-memory, no disk persistence yet
}

func NewC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				if !ok {
					panic("Note not found in pages!")
				}
				return node
			},
			new: func(node BNode) uint64 {
				if node.nbytes() > BTREE_PAGE_SIZE {
					panic("Node too large!")
				}
				key := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				if pages[key].data != nil {
					panic("Data at key is not null")
				}
				pages[key] = node
				return key
			},
			del: func(ptr uint64) {
				_, ok := pages[ptr]
				if !ok {
					panic("Note not found in pages!")
				}
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) Add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func (c *C) Del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}
