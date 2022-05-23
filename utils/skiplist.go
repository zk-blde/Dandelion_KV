package utils

import (
	"fmt"
	"github.com/pkg/errors"
	"log"
	"math"
	"strings"
	"sync/atomic"
	_ "unsafe"
)


const (
	maxHeight = 20
	heightIncrease = math.MaxUint32 / 3
)

type node struct {
	// 存储value offect 和 value size 拼接
	value uint64 
	keyOffset uint32
	keySize  uint16
	height   uint16 // 表示跳表的那一层
	tower   [maxHeight]uint32 // 所有节点的next指针
}


type Skiplist struct {
	height   int32 // 所处的那一层, 和key的最大层实现 cas操作
	headOffset  uint32  // 头节点
	ref         int32   // 引用计数
	arena     *Arena
	OnClose    func()
}

// 引用计数加一
func (s *Skiplist) IncrRef() {
	atomic.AddInt32(&s.ref, 1)
}

func (s *Skiplist) DecrRef() {
	newRef := atomic.AddInt32(&s.ref, -1)
	if newRef > 0 {
		return
	}
	if s.OnClose != nil {
		s.OnClose()
	}

	// 把这块内存回收
	s.arena = nil
}


func newNode(arena *Arena, key []byte, v ValueStruct, height int) *node {
	nodeOffset := arena.putNode(height)
	keyOffset := arena.put(key)
	val := encodeValue(arena.putVal(v), v.EncodedSize())

	node := arena.getNode(nodeOffset)
	node.keyOffset = keyOffset
	node.keySize = uint16(len(key))
	node.height = uint16(height)
	node.value = val
	return node
}

func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}

func NewSkipList(arenaSize int64) *Skiplist {
	arena := newArena(arenaSize)
	head := newNode(arena, nil, ValueStruct{}, maxHeight)

	ho := arena.getNodeOffset(head)
	return &Skiplist{
		height: 1,
		headOffset: ho,
		arena:  arena,
		ref: 1,
	}
}

func (n *node) getValueOffset() (uint32, uint32) {
	value := atomic.LoadInt64(&s.value)
	return decodeValue(value)
}

func (n *node) setValue(arena *Arena, vo uint64) {
	atomic.StoreUint64(&n.value, vo)
}

func (n *node) key(arena *Arena) []byte {
	return arena.getKey(s.keyOffset, s.keySize)
}

func (n *node) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&s.tower[h])
}

func (n *node) casNextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&s.tower[h], old, val)
}

func (n *node) getVs(arena *Arena) ValueStruct {
	valOffset, valSize := n.getValueOffset()
	return arena.getVal(valOffset, valSize)
}


func (s *Skiplist) randomHeight() int {
	h := 1
	for h < maxHeight && FastRand() <= heightIncrease {
		h++
	}
	return h
}


func (s *Skiplist) getNext(nd *node, height int) *node {
	return s.arena.getNode(nd.getNextOffset(height))
}

func (s *Skiplist) getHead() *node {
	return s.arena.getNode(s.headOffset)
}

func (s *Skiplist) Add (e *Entry) {
	key, v := e.Key, ValueStruct {
		Meta: e.Meta,
		Value: e.Value,
		ExpiresAt: e.ExpiresAt,
		Version: e.Version,
	}

	listHeight := s.getHeight()
	var prev [maxHeight + 1]uint32
	var next [maxHeight + 1]uint32
	prev[listHeight] = s.headOffset
	for i := int(listHeight) - 1; i >= 0; i-- {
		prev[i], next[i] = s.findSpliceForLevel(key, prev[i + 1], i)
		if prev[i] == next[i] {
			vo : = s.arena.putVal(v)
			encValue := encodeValue(vo, v.EncodeSize())
			prevNode := s.arena.getNode(prev[i])
			prevNode.setValue(s.arena, encValue)
			return
		}
	}
}











//go:linkname FastRand runtime.fastrand
func FastRand() uint32