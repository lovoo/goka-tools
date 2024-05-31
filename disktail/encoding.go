package disktail

import (
	"encoding/binary"
	"time"
)

const (
	offsetKeyLen = 4
	dataKeyLen   = 20

	// Number of bytes to be used to write the length of the key.
	// That means a key can only be 32bit long (4gb)
	valueKeylenSize = 4
)

// encodes key and value pair into its own key
func encodeValue(key, value []byte) []byte {

	buf := make([]byte, valueKeylenSize+len(key)+len(value)) // buffer to hold [<keylen><key><value>]
	binary.BigEndian.PutUint32(buf, uint32(len(key)))
	// copy key bytes
	copy(buf[valueKeylenSize:], key)
	// copy value bytes
	copy(buf[valueKeylenSize+len(key):], value)
	return buf
}
func decodeValue(encoded []byte) ([]byte, []byte) {
	keylen := binary.BigEndian.Uint32(encoded)

	// extract key and value
	return encoded[valueKeylenSize : valueKeylenSize+keylen], encoded[valueKeylenSize+keylen:]
}

// encodes the key to be used on disk absed on timestamp, offset and partition.
// the original key will be part of the value
func encodeKey(timestamp time.Time, offset int64, partition int32) []byte {
	b := make([]byte, dataKeyLen)

	binary.BigEndian.PutUint64(b, uint64(timestamp.UnixMicro()))
	binary.BigEndian.PutUint64(b[8:], uint64(offset))
	binary.BigEndian.PutUint32(b[16:], uint32(partition))
	return b
}

// decodes a key into its original parts
// - timestamp
// - offset
// - partition
func decodeKey(key []byte) (time.Time, int64, int32) {
	if len(key) != dataKeyLen {
		panic("invalid key length. drop the disk")
	}
	return time.UnixMicro(int64(binary.BigEndian.Uint64(key))), // timestamp
		int64(binary.BigEndian.Uint64(key[8:])), // offset
		int32(binary.BigEndian.Uint32(key[16:])) // partition
}

func offsetKeyForPartition(partition int32) []byte {
	buf := make([]byte, offsetKeyLen)
	binary.BigEndian.PutUint32(buf, uint32(partition))

	return buf
}
