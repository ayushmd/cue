package cue

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestItemEncodeDecode(t *testing.T) {
	// Sample data payload (raw JSON)
	rawJSON := `{"name":"Alice","age":30}`

	original := Item{
		Id:        123,
		QueueName: "test-queue",
		Data:      json.RawMessage([]byte(rawJSON)),
		TTL:       60,
		Retries:   2,
	}

	// Encode the item
	encoded, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode it back
	var decoded Item
	err = decoded.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Check values
	if original.Id != decoded.Id {
		t.Errorf("Id mismatch: got %d, want %d", decoded.Id, original.Id)
	}
	if original.QueueName != decoded.QueueName {
		t.Errorf("QueueName mismatch: got %s, want %s", decoded.QueueName, original.QueueName)
	}
	if !reflect.DeepEqual(original.Data, decoded.Data) {
		t.Errorf("Data mismatch: got %s, want %s", decoded.Data, original.Data)
	}
	if original.TTL != decoded.TTL {
		t.Errorf("TTL mismatch: got %d, want %d", decoded.TTL, original.TTL)
	}
	if original.Retries != decoded.Retries {
		t.Errorf("Retries mismatch: got %d, want %d", decoded.Retries, original.Retries)
	}
}
