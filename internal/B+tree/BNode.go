package BPlusTree

import (
	"encoding/binary"
	"log"
)

const (
	HEADER             = 4
	BTREE_PAGE_SIZE    = 4096
	KLEN_SIZE          = 2
	VLEN_SIZE          = 2
	KV_META_SIZE       = 4
	BTREE_MAX_KEY_SIZE = 1000
	BTRE_MAX_VAL_SIZE  = 3000
)

type BNode struct {
	data []byte
}

// defines the different sets of operations on the BNode
type BNodeInf interface {
	bytpe() uint16
	nkeys() uint16
	setHeader(btype uint16, nkeys uint16)
	getPtr(idx uint16) uint16
	setPtr(idx uint16)
	offSetPos(idx uint16)
	getOffSetPos(idx uint16) uint16
	setOffSet(idx uint16, offSet uint16)
	kvPos(idx uint16) uint16
	getKey(idx uint16) []byte
	getVal(idx uint16) []byte
	nbytes() uint16
}

func init() {
	node1max := (HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTRE_MAX_VAL_SIZE)
	if node1max > BTREE_PAGE_SIZE {
		log.Fatal("Error Node Size Must be less than Page Size")
	}
}

// returns the type of the BNode (node is leaf or not)
func (node BNode) bytpe() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

// returns the nkeys the number of keys in the Node
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

// sets the btype and the nkeys in the byte array
func (node BNode) setHeader(nkeys uint16, btype uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// returns pointer at a specific index
func (node BNode) getPtr(idx uint16) uint16 {
	if idx > node.nkeys() { // invalid index provided
		log.Fatal("Invalid index provided for the Btree")
	}
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint16(node.data[pos:])
}

// setPtr - sets/updates the pointer at a particular index
func (node BNode) setPtr(idx uint16, val uint16) {
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint16(node.data[pos:], val)
}

// returns the Offset of a particular kV pair
func (node BNode) OffsetPosition(idx uint16) uint16 {
	offset := HEADER + 8*node.nkeys() + 2*(idx-1)
	return offset
}

// gets the offset position
func (node BNode) getOffSetPos(idx uint16) uint16 {
	if idx > node.nkeys() {
		log.Fatal("Invalid Index Provided")
	}
	offsetPos := node.getOffSetPos(idx)
	return binary.LittleEndian.Uint16(node.data[offsetPos:])
}

func (node BNode) setOffSet(idx uint16, offset uint16) {
	if idx > node.nkeys() {
		log.Fatal("Invalid index is provided.")
	}
}

// returns the position of the KV pair
func (node BNode) kvPos(idx uint16) uint16 {
	pos := HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffSetPos(idx)
	return pos
}

// return the key
func (node BNode) getKey(idx uint16) uint16 {
	klenpos := node.kvPos(idx)
	keysize := binary.LittleEndian.Uint16(node.data[klenpos : klenpos+KLEN_SIZE])
	keyval := binary.LittleEndian.Uint16(node.data[klenpos+KV_META_SIZE : klenpos+KV_META_SIZE+keysize])
	return keyval
}

func (node BNode) getVal(idx uint16) uint16 {
	klenpos := node.kvPos(idx)
	keysize := binary.LittleEndian.Uint16(node.data[klenpos : klenpos+KLEN_SIZE])
	valsize := binary.LittleEndian.Uint16(node.data[klenpos+KLEN_SIZE:])
	value := binary.LittleEndian.Uint16(node.data[klenpos+KV_META_SIZE+keysize:][:valsize])
	return value
}

func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}
