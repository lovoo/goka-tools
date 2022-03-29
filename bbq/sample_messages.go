package bbq

type MessageA struct {
	A string
	B int32
	C *MessageB
	D []*MessageB
	E MessageC
}

type MessageB struct {
	A int32
	B map[string]string
}

type MessageC int32

const (
	MessageC_UNSPECIFIED MessageC = 0
	MessageC_OTHER       MessageC = 1
)

// Enum value maps for Platform.
var (
	MessageC_name = map[int32]string{
		0: "UNSPECIFIED",
		1: "OTHER",
	}
	MessageC_value = map[string]int32{
		"UNSPECIFIED": 0,
		"OTHER":       1,
	}
)

func (x MessageC) Enum() *MessageC {
	p := new(MessageC)
	*p = x
	return p
}

type BytesCodec struct{}

func (d *BytesCodec) Encode(value interface{}) ([]byte, error) {
	data, _ := value.([]byte)
	return data, nil
}

func (d *BytesCodec) Decode(data []byte) (interface{}, error) {
	return data, nil
}
