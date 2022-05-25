package utils

import (
	"encoding/binary"
	"time"
)

type ValueStruct struct {
	Meta      byte
	Value     []byte
	ExpiresAt uint64

	Version uint64 // This field is not serialized. Only for internal usage.
}

// 对value 和过期时间进行编码
func (vs *ValueStruct) EncodedSize() uint32 {
	sz := len(vs.Value) + 1
	enc := sizeVarint(vs.ExpiresAt)
	return uint32(sz + enc)
}

func (vs *ValueStruct) DecodeValue(buf []byte) {
	vs.Meta = buf[0]
	var sz int
	//  binary.Uvarint(buf[1:]) 有疑问
	vs.ExpiresAt, sz = binary.Uvarint(buf[1:])
	vs.Value = buf[1+sz:]
}

func (vs *ValueStruct) EncodeValue(b []byte) uint32 {
	b[0] = vs.Meta
	sz := binary.PutUvarint(b[1:], vs.ExpiresAt)
	n := copy(b[1+sz:], vs.Value)
	return uint32(1 + sz + n)
}

// sizeVarint 这个是计算过期实际占用几个字节
func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64

	Meta         byte
	Version      uint64
	Offset       uint32
	Hlen         int // Length of the header.
	ValThreshold int64
}

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

func (e *Entry) Entry() *Entry {
	return e
}


func (e *Entry) IsDeletedOrExpired() bool {
	// 如果value是nil 返回true
	if e.Value == nil {
		return true
	}
	// 过期时间等于0代表持久化
	if e.ExpiresAt == 0 {
		return false
	}
	// 和当前的时间戳做对比
	return e.ExpiresAt <= uint64(time.Now().Unix())
}

// 查看当前的ttl
func (e *Entry) WithTTL (dur time.Duration) *Entry {
	e.ExpiresAt = uint64(time.Now().Add(dur).Unix())
	return e
}

// ValueStruct的size
func (e *Entry) EncodedSize() uint32 {
	sz := len(e.Value)
	enc := sizeVarint(uint64(e.Meta))
	enc += sizeVarint(e.ExpiresAt)
	return uint32(sz + enc)
}

func (e *Entry) EstimateSize(threshold int) int {
	if len(e.Value) < threshold {
		return len(e.Key) + len(e.Value) + 1// 1指meta
	}
	return len(e.Key) + 12 + 1
}

// 头结点
type Header struct {
	KLen      uint32
	VLen      uint32
	ExpiresAt uint64
	Meta      byte
}

// +------+----------+------------+--------------+-----------+
// | Meta | UserMeta | Key Length | Value Length | ExpiresAt |
// +------+----------+------------+--------------+-----------+
func (h Header) Encode(out []byte) int {
	// 这里的Meta是一个字节
	out[0] = h.Meta
	index := 1
	index += binary.PutUvarint(out[index:], uint64(h.KLen))
	index += binary.PutUvarint(out[index:], uint64(h.VLen))
	index += binary.PutUvarint(out[index:], h.ExpiresAt)
	return index
}

// 从header对象字节块读取字节数
func (h *Header) Decode(buf []byte) int {
	h.Meta = buf[0]
	index := 1 
	klen, count := binary.Uvarint(buf[index:])
	h.KLen = uint32(klen)
	index += count
	vlen, count := binary.Uvarint(buf[index:])
	h.VLen = uint32(vlen)
	index += count
	h.ExpiresAt, count = binary.Uvarint(buf[index:])
	return index + count
}


func (h *Header) DecodeFrom(reader *HashReader) (int, error) {
	var err error
	h.Meta, err = reader.ReadByte()
	if err != nil {
		return 0, err
	}

	// todo ?????
	klen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.KLen = uint32(klen)
	vlen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.VLen = uint32(vlen)
	h.ExpiresAt, err = binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	return reader.BytesRead, nil
}