package btree

import (
	"bytes"
	"encoding/binary"
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

// returns the first kid node whose range intersects the key. (kid[i] <= key)
// TODO: bisect
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys() // get how many keys in node
	found := uint16(0)    // initialize to first key

	// start at index 1, if key is greater than current index in node, quit (meaning to add in that index and push everything up)
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

// add a new key to a leaf node
func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)                   //copy everything from old node to new up until index
	nodeAppendKV(new, idx, 0, key, val)                    //add new kv to new node
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx) //copy everything remaining from index of old to new node starting after the inserted kv
}

// add a new key to a leaf node
func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)                         //copy everything from old node to new up until index
	nodeAppendKV(new, idx, 0, key, val)                          //add new kv to new node
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1)) //copy everything remaining from index of old to new node starting after the inserted kv
}

// copy multiple KVs into the position
func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	if srcOld+n <= old.nkeys() {
		panic("Index out of bounds!")
	}
	if dstNew+n <= new.nkeys() {
		panic("Index out of bounds!")
	}

	if n == 0 {
		return
	}

	// pointers
	for i := uint16(0); i < n; i++ {
		new.setPtr(dstNew+i, old.getPtr(srcOld+i))
	}
	// offsets
	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)
	for i := uint16(1); i <= n; i++ { // NOTE: the range is [1, n]
		offset := dstBegin + old.getOffset(srcOld+i) - srcBegin
		new.setOffset(dstNew+i, offset)
	}
	// KVs
	begin := old.kvPos(srcOld)
	end := old.kvPos(srcOld + n)
	copy(new.data[new.kvPos(dstNew):], old.data[begin:end])
}

// copy a KV into the position
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.setPtr(idx, ptr)
	// KVs
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new.data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(val)))
	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+uint16(len(key)):], val)
	// the offset of the next key
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16((len(key)+len(val))))
}

// insert a KV into a node, the result might be split into 2 nodes.
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes.
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// the result node.
	// it's allowed to be bigger than 1 page and will be split if so
	new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}

	// where to insert the key?
	idx := nodeLookupLE(node, key)
	// act depending on the node type
	switch node.btype() {
	case BNODE_LEAF:
		// leaf, node.getKey(idx) <= key
		if bytes.Equal(key, node.getKey(idx)) {
			// found the key, update it.
			leafUpdate(new, node, idx, key, val)
		} else {
			// insert it after the position.
			leafInsert(new, node, idx+1, key, val)
		}
	case BNODE_NODE:
		// internal node, insert it to a kid node.
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("bad node!")
	}
	return new
}

// part of the treeInsert(): KV insertion to an internal node
func nodeInsert(tree *BTree, new BNode, node BNode, idx uint16, key []byte, val []byte) {
	// get and deallocate the kid node
	kptr := node.getPtr(idx)
	knode := tree.get(kptr)
	tree.del(kptr)
	// recursive insertion to the kid node
	knode = treeInsert(tree, knode, key, val)
	// split the result
	nsplit, splited := nodeSplit3(knode)
	// update the kid links
	nodeReplaceKidN(tree, new, node, idx, splited[:nsplit]...)
}

// func nodeSplit2(left BNode, right BNode, old BNode) {
// 	// code omitted...
// }

// func nodeSplit3(old BNode) (uint16, [3]BNode) {
// 	if old.nbytes() <= BTREE_PAGE_SIZE {
// 		old.data = old.data[:BTREE_PAGE_SIZE]
// 		return 1, [3]BNode{old}
// 	}
// 	left := BNode{make([]byte, 2*BTREE_PAGE_SIZE)} // might be split later
// 	right := BNode{make([]byte, BTREE_PAGE_SIZE)}
// 	nodeSplit2(left, right, old)
// 	if left.nbytes() <= BTREE_PAGE_SIZE {
// 		left.data = left.data[:BTREE_PAGE_SIZE]
// 		return 2, [3]BNode{left, right}
// 	}
// 	// the left node is still too large
// 	leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
// 	middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
// 	nodeSplit2(leftleft, middle, left)

// 	if leftleft.nbytes() <= BTREE_PAGE_SIZE {
// 		panic("Index out of bounds!")
// 	}
// 	return 3, [3]BNode{leftleft, middle, right}
// }

// // replace a link with multiple links
// func nodeReplaceKidN(
// 	tree *BTree, new BNode, old BNode, idx uint16,
// 	kids ...BNode,
// ) {
// 	inc := uint16(len(kids))
// 	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
// 	nodeAppendRange(new, old, 0, 0, idx)
// 	for i, node := range kids {
// 		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
// 	}
// 	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
// }
