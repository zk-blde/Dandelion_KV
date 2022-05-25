package utils

import (
	"github.com/pkg/errors"
	"log"
	"sync/atomic"
	"unsafe"
)

const (
	offesetSize = int(unsafe.Sizeof(uint32(0)))
	//始终对齐64位边界上的节点，即使是在32位架构上，
	//使节点。Value字段为64位对齐。这是必要的，因为
	//节点。getValueOffset使用原子。LoadUint64期望输入值
	// 64位对齐的指针。
	nodeAlign = int(unsafe.Sizeof(uint(0))) - 1
	MaxNodeSize = int(unsafe.Sizeof(node{}))
)

type Arena struct {
	n  uint32
	//是否允许扩容, 默认是true, 如果要控制arena的大小, 则改为false
	shouldGrow  bool  
	buf         []byte
}

func newArena(n int64) *Arena {
	out := &Arena{
		n: 1,
		buf: make([]byte, n),
	}
	return out
}

func (s *Arena) allocate(sz uint32) uint32 {
	offset := atomic.AddUint32(&s.n, sz)
	if !s.shouldGrow {
		AssertTrue(int(offset) <= len(s.buf))
		return offset - sz
	}

	if int(offset) > len(s.buf) - MaxNodeSize {
		growBy := uint32(len(s.buf))
		if growBy > 1 << 30 {
			growBy = 1 << 30
		}
		if growBy < sz {
			growBy = sz
		}
		newBuf := make([]byte, len(s.buf)+int(growBy))
		
		AssertTrue(len(s.buf) == copy(newBuf, s.buf))
		s.buf = newBuf
	}
	return offset - sz
}

func (s *Arena) size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

func (s *Arena) putNode(height int) uint32 {
	unusedSize := (maxHeight - height) * offesetSize

	l := uint32(MaxNodeSize - unusedSize + nodeAlign)
	n := s.allocate(l)

	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
}

func (s *Arena) putVal(v ValueStruct) uint32 {
	l := uint32(v.EncodedSize())
	offset := s.allocate(l)
	v.EncodeValue(s.buf[offset:])
	return offset
}

func (s *Arena) putKey(key []byte) uint32 {
	keySz := uint32(len(key))
	offset := s.allocate(keySz)
	buf := s.buf[offset : offset+keySz]
	AssertTrue(len(key) == copy(buf, key))
	return offset
}

func (s *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	// todo 不理解
	return (*node)(unsafe.Pointer(&s.buf[offset]))
}

func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

func (s *Arena) getVal(offset uint32, size uint32) (ret ValueStruct) {
	ret.DecodeValue(s.buf[offset : offset+size])
	return
}

// 这里返回node 节点在arena的offset
func (s *Arena) getNodeOffset(nd *node) uint32 {
	if nd == nil {
		return 0
	}

	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}

