package disktail

import (
	"encoding/binary"
	"log"
	"time"
)

const (
	// length of a key in pebble.
	// A key includes timestamp, offset and partition of the original message
	keyLen = 20

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

var offsetKeyTime = time.Unix(0, 0)

func encodeOffsetKey(partition int32) []byte {
	return encodeKey(offsetKeyTime, 0, partition)
}

// encodes the key to be used on disk absed on timestamp, offset and partition.
// the original key will be part of the value
func encodeKey(timestamp time.Time, offset int64, partition int32) []byte {
	b := make([]byte, keyLen)
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
	if len(key) != keyLen {
		log.Printf("invalid key length. drop the disk: %s", string(key))
		return time.Time{}, 0, 0
	}
	return time.UnixMicro(int64(binary.BigEndian.Uint64(key))), // timestamp
		int64(binary.BigEndian.Uint64(key[8:])), // offset
		int32(binary.BigEndian.Uint32(key[16:])) // partition
}
