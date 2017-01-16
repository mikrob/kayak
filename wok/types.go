package wok

import (
	"bytes"
	"errors"
	"fmt"

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
	fmt.Printf("Decoding %T...\n", wt)
	wt.Decode(unpacker)
	fmt.Printf("Decoded %T %v\n", wt, wt)
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

//WFloat represent a woklist
type WFloat struct {
	Value float64
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

//WInt represent a wok int
type WInt struct {
	Value int64
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

//Decode allow to decode a wok unsigned int into a golang int32
func (wi *WUInt) Decode(unpacker *binpacker.Unpacker) error {
	var sizeByte byte
	unpacker.FetchByte(&sizeByte)
	fmt.Println("Int Byte size :", sizeByte)
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

//Decode allow to decode a wok string
func (ws *WBinary) Decode(unpacker *binpacker.Unpacker) error {
	wt := DecodeValue(unpacker)
	sizeInt := wt.(*WUInt)
	unpacker.FetchBytes(sizeInt.Value, &ws.Value)
	return nil
}

//WMap represent a wok map
type WMap struct {
	Values map[WType]WType
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
}

//DecodeMessage allow to decode a message
func (wm *Message) DecodeMessage() {
	buffer := new(bytes.Buffer)
	packer := binpacker.NewPacker(buffer)
	packer.PushBytes(wm.Binary)
	unpacker := binpacker.NewUnpacker(buffer)
	unpacker.FetchByte(&wm.Version)
	if wm.Version != 1 {
		fmt.Println("Not a wok message")
	} else {
		wm.To = *DecodeValue(unpacker).(*WList)
		wm.From = *DecodeValue(unpacker).(*WBinary)
		wm.UUID = *DecodeValue(unpacker).(*WBinary)
		wm.Headers = *DecodeValue(unpacker).(*WMap)
		unpacker.FetchInt32(&wm.CRCHeader)
		unpacker.FetchInt32(&wm.CRCBody)
		body := make([]byte, len(wm.Binary))
		n, err := buffer.Read(body)
		if err != nil {
			fmt.Println("Error while reading body in buffer")
		}
		wm.Body = body[0:n]
	}
}
