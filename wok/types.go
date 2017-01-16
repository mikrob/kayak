package wok

import (
	"bytes"
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
	Decode(content []byte)
}

//WList represent a woklist
type WList struct {
	Values []interface{}
}

//WString represent a wok string
type WString struct {
	Value string
}

//WMap represent a wok map
type WMap struct {
	Values map[interface{}]interface{}
}

//Decode allow to decode
func (wt *WList) Decode(content []byte) {
	fmt.Println("Decode list")
}

//Message represent a wok message
type Message struct {
	binary    []byte
	Version   byte
	To        WList
	From      WString
	UUID      WString
	Headers   WMap
	CRCHeader int32
	CRCBody   int32
	Body      string
}

//DecodeMessage allow to decode a message
func (wm *Message) DecodeMessage() {
	buffer := new(bytes.Buffer)
	packer := binpacker.NewPacker(buffer)
	packer.PushBytes(wm.binary)
	unpacker := binpacker.NewUnpacker(buffer)
	unpacker.FetchByte(&wm.Version)
	//unpacker.FetchByte(&wm.Type)

}
