package bbq

type MessageA struct {
	A string
	B int32
	C *MessageB
	D []*MessageB
}

type MessageB struct {
	A int32
	B map[string]string
}

type BytesCodec struct{}

func (d *BytesCodec) Encode(value interface{}) ([]byte, error) {
	data, _ := value.([]byte)
	return data, nil
}

func (d *BytesCodec) Decode(data []byte) (interface{}, error) {
	return data, nil
}
