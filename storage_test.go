package main

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func setupDB(t *testing.T) *DataStorage {
	// Remove old db if any
	os.RemoveAll("demo")
	ds := NewDataStorage()
	t.Cleanup(func() {
		ds.db.Close()
		os.RemoveAll("demo")
	})
	return ds
}

// func TestCreateAndGetQueues(t *testing.T) {
// 	ds := setupDB(t)

// 	err := ds.CreateQueue("queue1")
// 	assert.NoError(t, err)

// 	err = ds.CreateQueue("queue2")
// 	assert.NoError(t, err)

// 	queues, err := ds.GetQueues()
// 	assert.NoError(t, err)
// 	assert.Len(t, queues, 2)
// }

func TestCreateItemSyncAndPeek(t *testing.T) {
	ds := setupDB(t)

	item := Item{
		TTL:  int(time.Now().Add(10 * time.Second).UnixMilli()),
		Data: "test_value",
	}
	err := ds.CreateItemSync(item)
	assert.NoError(t, err)

	fetchedItem, err := ds.PeekItem()
	assert.NoError(t, err)
	assert.Equal(t, item.Data, fetchedItem.Data)
}

func TestCreateItemAsyncAndPeekTTL(t *testing.T) {
	ds := setupDB(t)

	item := Item{
		TTL:  int(time.Now().Add(15 * time.Second).UnixMilli()),
		Data: "async_value",
	}
	err := ds.CreateItemAsync(item)
	assert.NoError(t, err)

	ttl, err := ds.PeekTTL()
	assert.NoError(t, err)
	assert.Equal(t, int64(item.TTL), ttl)
}

func TestItemsSized(t *testing.T) {
	ds := setupDB(t)

	now := time.Now()

	item1 := Item{TTL: int(now.Add(5 * time.Second).UnixMilli()), Data: "item1"}
	item2 := Item{TTL: int(now.Add(20 * time.Second).UnixMilli()), Data: "item2"}

	_ = ds.CreateItemSync(item1)
	_ = ds.CreateItemSync(item2)

	// Should retrieve only item1 if we limit to 10 seconds
	items, err := ds.ItemsSized(10)
	assert.NoError(t, err)
	assert.Len(t, items, 1)
	assert.Equal(t, "item1", items[0].Data)
}

func TestItemsSized2(t *testing.T) {
	ds := setupDB(t)

	now := time.Now()

	item1 := Item{TTL: int(now.Add(5 * time.Second).UnixMilli()), Data: "item1"}
	item2 := Item{TTL: int(now.Add(20 * time.Second).UnixMilli()), Data: "item2"}

	_ = ds.CreateItemSync(item1)
	_ = ds.CreateItemSync(item2)

	// Should retrieve only item1 if we limit to 10 seconds
	items, err := ds.ItemsSized(20)
	assert.NoError(t, err)
	assert.Len(t, items, 2)
	assert.Equal(t, "item1", items[0].Data)
	assert.Equal(t, "item2", items[1].Data)
}

func TestDeleteItemRange(t *testing.T) {
	ds := setupDB(t)

	now := time.Now()
	item1 := Item{TTL: int(now.Add(5 * time.Second).UnixMilli()), Data: "item1"}
	item2 := Item{TTL: int(now.Add(10 * time.Second).UnixMilli()), Data: "item2"}
	item3 := Item{TTL: int(now.Add(15 * time.Second).UnixMilli()), Data: "item3"}

	_ = ds.CreateItemSync(item1)
	_ = ds.CreateItemSync(item2)
	_ = ds.CreateItemSync(item3)

	start := now.Add(5 * time.Second).UnixMilli()
	end := now.Add(15 * time.Second).UnixMilli()

	err := ds.DeleteItemRange(start, end)
	assert.NoError(t, err)

	items, err := ds.ItemsSized(30)
	assert.NoError(t, err)

	assert.Len(t, items, 1) // Only item3 should remain
	assert.Equal(t, "item3", items[0].Data)
}
