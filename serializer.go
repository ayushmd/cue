package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
)

type Item struct {
	Id        int64  `json:"id"`
	QueueName string `json:"queueName"`
	Data      []byte `json:"data"`
	TTL       int64  `json:"ttl"`
	Retries   uint8  `json:"retries"`
}

func (i Item) Encode() ([]byte, error) {
	if debug {
		return json.Marshal(i)
	}
	var buf bytes.Buffer

	// Write Id
	if err := binary.Write(&buf, binary.LittleEndian, i.Id); err != nil {
		return nil, err
	}

	// Write QueueName
	queueNameBytes := []byte(i.QueueName)
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(queueNameBytes))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(queueNameBytes); err != nil {
		return nil, err
	}

	// Write Data (as raw JSON bytes)
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(i.Data))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(i.Data); err != nil {
		return nil, err
	}

	// Write TTL and Retries
	if err := binary.Write(&buf, binary.LittleEndian, i.TTL); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, i.Retries); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (i *Item) Decode(data []byte) error {
	if debug {
		return json.Unmarshal(data, i)
	}
	buf := bytes.NewReader(data)

	// Read Id
	if err := binary.Read(buf, binary.LittleEndian, &i.Id); err != nil {
		return err
	}

	// Read QueueName
	var nameLen int32
	if err := binary.Read(buf, binary.LittleEndian, &nameLen); err != nil {
		return err
	}
	nameBytes := make([]byte, nameLen)
	if _, err := io.ReadFull(buf, nameBytes); err != nil {
		return err
	}
	i.QueueName = string(nameBytes)

	// Read Data
	var dataLen int32
	if err := binary.Read(buf, binary.LittleEndian, &dataLen); err != nil {
		return err
	}
	dataBytes := make([]byte, dataLen)
	if _, err := io.ReadFull(buf, dataBytes); err != nil {
		return err
	}
	i.Data = json.RawMessage(dataBytes)

	// Read TTL and Retries
	if err := binary.Read(buf, binary.LittleEndian, &i.TTL); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &i.Retries); err != nil {
		return err
	}

	return nil
}
