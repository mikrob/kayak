package wok

import (
	"bytes"
	"compress/zlib"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/zhuangsirui/binpacker"
)

const (
	//TypeString is a string representation
	TypeString = 1
	//TypeInt is a int representation
	TypeInt = 10
	//TypeUint is uint representation
	TypeUint = 11
	//TypeFloat is a float representation
	TypeFloat = 20
	//TypeList is a list representation
	TypeList = 30
	//TypeMap is a map representation
	TypeMap = 40
	//TypeBooleanTrue represent true
	TypeBooleanTrue = 50
	//TypeBooleanFalse represent false
	TypeBooleanFalse = 51
	//TypeAtom represent an erlang atom
	TypeAtom = 60
)

//WType is the interface mother of all WokType
type WType interface {
	Decode(unpacker *binpacker.Unpacker) error
	ToGo() interface{}
}

//DecodeValue is the generic function to read wok values
func DecodeValue(unpacker *binpacker.Unpacker) WType {
	var typeByte byte
	unpacker.FetchByte(&typeByte)
	var wt WType
	switch typeByte {
	case TypeInt:
		wt = &WInt{}
	case TypeUint:
		wt = &WUInt{}
	case TypeString:
		wt = &WBinary{}
	case TypeAtom:
		wt = &WBinary{}
	case TypeList:
		wt = &WList{}
	case TypeMap:
		wt = &WMap{}
	case TypeFloat:
		wt = &WFloat{}
	case TypeBooleanFalse:
		wt = &WBool{Value: false}
	case TypeBooleanTrue:
		wt = &WBool{Value: true}
	default:
		return nil
	}
	//fmt.Printf("Decoding %T...\n", wt)
	wt.Decode(unpacker)
	//fmt.Printf("Decoded %T %v\n", wt, wt)
	return wt

}

//WBool represent a bool
type WBool struct {
	Value bool
}

//Decode bool
func (wb *WBool) Decode(unpacker *binpacker.Unpacker) error {
	unpacker.ShiftByte()
	return nil
}

//ToGo Convert the type to go type
func (wb *WBool) ToGo() interface{} {
	return wb.Value
}

//WFloat represent a WokFloat
type WFloat struct {
	Value float64
}

//ToGo Convert the type to go type
func (wf *WFloat) ToGo() interface{} {
	return wf.Value
}

//Decode a float
func (wf *WFloat) Decode(unpacker *binpacker.Unpacker) error {
	return nil
}

//WList represent a woklist
type WList struct {
	Values []WType
}

//Decode allow to decode
func (wl *WList) Decode(unpacker *binpacker.Unpacker) error {
	sizeInt := int(DecodeValue(unpacker).(*WUInt).Value)
	wl.Values = make([]WType, sizeInt)
	for i := 0; i < sizeInt; i++ {
		wl.Values[i] = DecodeValue(unpacker)
	}
	return nil
}

//ToStringList convert a wlist to go list
func (wl *WList) ToStringList() []string {
	list := make([]string, len(wl.Values))
	for idx, val := range wl.Values {
		list[idx] = (val.(*WBinary)).ToString()
	}
	return list
}

//ToGo represent go type
func (wl *WList) ToGo() interface{} {
	result := make([]interface{}, len(wl.Values))
	for idx, val := range wl.Values {
		result[idx] = val.ToGo()
	}
	return result
}

//WInt represent a wok int
type WInt struct {
	Value int64
}

//ToGo return the go value of a wok type
func (wi *WInt) ToGo() interface{} {
	return wi.Value
}

//Decode allow to decode a wok int into a golang int32
func (wi *WInt) Decode(unpacker *binpacker.Unpacker) error {
	var sizeByte byte
	unpacker.FetchByte(&sizeByte)
	fmt.Println("Int Byte size :", sizeByte)
	var value int64
	switch sizeByte {
	case 8:
		var b byte
		unpacker.FetchByte(&b)
		if b > 127 {
			value = -(256 - int64(b))
		} else {
			value = int64(b)
		}
	case 16:
		var i16 int16
		unpacker.FetchInt16(&i16)
		value = int64(i16)
	case 32:
		var i32 int32
		unpacker.FetchInt32(&i32)
		value = int64(i32)
	case 64:
		unpacker.FetchInt64(&value)
	default:
		return errors.New("Don't known how to fetch this wok int")
	}
	wi.Value = value
	return nil
}

//WUInt represent a wok int
type WUInt struct {
	Value uint64
}

//ToGo return the go value of a wok type
func (wi *WUInt) ToGo() interface{} {
	return wi.Value
}

//Decode allow to decode a wok unsigned int into a golang int32
func (wi *WUInt) Decode(unpacker *binpacker.Unpacker) error {
	var sizeByte byte
	unpacker.FetchByte(&sizeByte)
	var value uint64
	switch sizeByte {
	case 8:
		var b byte
		unpacker.FetchByte(&b)
		value = uint64(b)
	case 16:
		var i16 uint16
		unpacker.FetchUint16(&i16)
		value = uint64(i16)
	case 32:
		var i32 uint32
		unpacker.FetchUint32(&i32)
		value = uint64(i32)
	case 64:
		unpacker.FetchUint64(&value)
	default:
		return errors.New("Don't known how to fetch this wok uint")
	}
	wi.Value = value
	return nil
}

//WBinary represent a wok binary
type WBinary struct {
	Value []byte
}

//ToGo return a WSBinary in a go string
func (ws *WBinary) ToGo() interface{} {
	return ws.ToString()
}

//Decode allow to decode a wok string
func (ws *WBinary) Decode(unpacker *binpacker.Unpacker) error {
	wt := DecodeValue(unpacker)
	sizeInt := wt.(*WUInt)
	unpacker.FetchBytes(sizeInt.Value, &ws.Value)
	return nil
}

//ToString convert a WBinary to a go String
func (ws *WBinary) ToString() string {
	return string(ws.Value)
}

//WMap represent a wok map
type WMap struct {
	Values map[WType]WType
}

//ToMap transform a WMAp to map<string>interface
func (wm *WMap) ToMap() map[string]interface{} {
	mapResult := make(map[string]interface{})
	for key, value := range wm.Values {
		mapResult[key.(*WBinary).ToString()] = value.ToGo()
	}
	return mapResult
}

//ToGo represent a WMap into a go map
func (wm *WMap) ToGo() interface{} {
	return wm.ToMap()
}

//Decode allow to decode a wmap
func (wm *WMap) Decode(unpacker *binpacker.Unpacker) error {
	sizeInt := int(DecodeValue(unpacker).(*WUInt).Value)
	wm.Values = make(map[WType]WType)
	for i := 0; i < sizeInt; i++ {
		key := DecodeValue(unpacker)
		value := DecodeValue(unpacker)
		wm.Values[key] = value
	}
	return nil
}

//Message represent a wok message
type Message struct {
	Binary    []byte
	Version   byte
	To        WList
	From      WBinary
	UUID      WBinary
	Headers   WMap
	CRCHeader int32
	CRCBody   int32
	Body      []byte
	Timestamp time.Time
}

//UncompressBody allow to zlib uncompress the body of message
func (m *Message) UncompressBody() {
	r, err := zlib.NewReader(bytes.NewReader(m.Body))
	// defer r.Close()
	if err != nil {
		fmt.Println("Error while zlib decompress of body", err.Error())
		return
	}
	result, errCopy := ioutil.ReadAll(r)
	if errCopy != nil {
		fmt.Println("Error while transfering data to byte array : ", errCopy.Error())
	}
	r.Close()
	m.Body = result
}

//GenericMessage represent a genericMessage
type GenericMessage struct {
	Version   int32
	To        []string
	From      string
	UUID      string
	Headers   map[string]interface{}
	CRCHeader int32
	CRCBody   int32
	Body      string
	Timestamp time.Time
}

//Stdout return a generic message formated for stdout
func (gm *GenericMessage) Stdout() string {
	return fmt.Sprintf("=================================== \nVersion : %d, To: %v, From: %s, UUID : %s, Headers : %v, CRCHeader : %d, CRCBody : %d \nBody : %s", gm.Version, gm.To, gm.From, gm.UUID, gm.Headers, gm.CRCHeader, gm.CRCBody, gm.Body)
}

//IsCompressed check if message is compressed or not
func (m *Message) IsCompressed() bool {
	headers := m.Headers.ToMap()
	var compressed bool
	for key, value := range headers {
		if key == "compress" {
			compressed = value.(bool)
		}
	}
	return compressed
}

//ToGenericMessage convert a wokmessage into a GenericMessage
func (m *Message) ToGenericMessage() GenericMessage {

	if m.IsCompressed() {
		m.UncompressBody()
	}

	gm := GenericMessage{
		Version:   int32(m.Version),
		To:        m.To.ToStringList(),
		From:      m.From.ToString(),
		UUID:      m.UUID.ToString(),
		Headers:   m.Headers.ToMap(),
		CRCHeader: m.CRCHeader,
		CRCBody:   m.CRCBody,
		Body:      string(m.Body),
		Timestamp: m.Timestamp,
	}
	return gm
}

//DecodeMessage allow to decode a message
func (m *Message) DecodeMessage() {
	buffer := new(bytes.Buffer)
	packer := binpacker.NewPacker(buffer)
	packer.PushBytes(m.Binary)
	unpacker := binpacker.NewUnpacker(buffer)
	unpacker.FetchByte(&m.Version)
	if m.Version != 1 {
		fmt.Println("Not a wok message")
	} else {
		m.To = *DecodeValue(unpacker).(*WList)
		m.From = *DecodeValue(unpacker).(*WBinary)
		m.UUID = *DecodeValue(unpacker).(*WBinary)
		m.Headers = *DecodeValue(unpacker).(*WMap)
		unpacker.FetchInt32(&m.CRCHeader)
		unpacker.FetchInt32(&m.CRCBody)
		m.Body = DecodeValue(unpacker).(*WBinary).Value
		m.Timestamp = time.Now()
	}
}
