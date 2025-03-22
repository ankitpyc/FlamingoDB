package BPlusTree

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
	offSetPos(node BNode, idx uint16)
	getOffSetPos(idx uint16) uint16
	setOffSet(idx uint16, offSet uint16)
	kvPos(idx uint16) uint16
	getKey(idx uint16) []byte
	getVal(idx uint16) []byte
	nbytes() uint16
}
