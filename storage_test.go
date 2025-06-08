package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func setupDB(t *testing.T) *DataStorage {
	// Remove old db if any
	os.RemoveAll("test")
	ds := NewDataStorage("test")
	t.Cleanup(func() {
		ds.db.Close()
		os.RemoveAll("test")
	})
	return ds
}

func TestCreateAndGetQueues(t *testing.T) {
	ds := setupDB(t)

	err := ds.CreateQueue("test")
	assert.NoError(t, err)

	err = ds.CreateQueue("asd")
	assert.NoError(t, err)

	queues, err := ds.GetQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 2)
}

func TestGetQueues(t *testing.T) {
	ds := setupDB(t)
	err := ds.CreateQueue("~badqueue") // '~' > 'z', so it is out of upper bound
	assert.NoError(t, err)

	// Insert a valid queue
	err = ds.CreateQueue("a") // 'a' is between '0' and 'z'
	assert.NoError(t, err)

	queues, err := ds.GetQueues()
	assert.NoError(t, err)
	fmt.Println(queues)

	// Should return only one valid queue
	assert.Len(t, queues, 1, "expected only one queue in bounds")
}

func TestMilliTimestamp(t *testing.T) {
	tmpstmp := time.Now().UnixMilli()
	var oldestEpoch int64 = time.Unix(0, 0).UnixMilli()
	fmt.Println(tmpstmp, oldestEpoch)
}

func TestItemInsertSequence(t *testing.T) {
	ds := setupDB(t)

	now := time.Now()
	arr := []Item{
		{TTL: now.Add(5 * time.Second).UnixMilli(), Data: []byte("item1")},
		{TTL: now.Add(10 * time.Second).UnixMilli(), Data: []byte("item2")},
		{TTL: now.Add(15 * time.Second).UnixMilli(), Data: []byte("item3")},
	}

	_ = ds.CreateItemSync(arr[0])
	_ = ds.CreateItemSync(arr[1])
	_ = ds.CreateItemSync(arr[2])
	i := 0
	it, _ := ds.db.NewIter(nil)
	for it.First(); it.Valid(); it.Next() {
		var item Item
		item.Decode(it.Value())
		assert.Equal(t, arr[i].Data, item.Data)
		i++
	}
}

func TestItemInsertSequenceDesc(t *testing.T) {
	ds := setupDB(t)

	now := time.Now()
	arr := []Item{
		{TTL: now.Add(15 * time.Second).UnixMilli(), Data: []byte("item1")},
		{TTL: now.Add(10 * time.Second).UnixMilli(), Data: []byte("item2")},
		{TTL: now.Add(5 * time.Second).UnixMilli(), Data: []byte("item3")},
	}

	_ = ds.CreateItemSync(arr[0])
	_ = ds.CreateItemSync(arr[1])
	_ = ds.CreateItemSync(arr[2])

	i := 2
	it, _ := ds.db.NewIter(nil)
	for it.First(); it.Valid(); it.Next() {
		var item Item
		item.Decode(it.Value())
		assert.Equal(t, arr[i].Data, item.Data)
		i--
	}
}
func TestCreateItemSyncAndPeek(t *testing.T) {
	ds := setupDB(t)

	item := Item{
		TTL:  time.Now().Add(10 * time.Second).UnixMilli(),
		Data: []byte("test_value"),
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
		TTL:  time.Now().Add(15 * time.Second).UnixMilli(),
		Data: []byte("async_value"),
	}
	// ds.CreateQueue("test")
	err := ds.CreateItemAsync(item)
	assert.NoError(t, err)

	ttl, err := ds.PeekTTL()
	assert.NoError(t, err)
	assert.Equal(t, int64(item.TTL), ttl)
}

func TestItemsSized(t *testing.T) {
	ds := setupDB(t)

	now := time.Now()

	item1 := Item{TTL: now.Add(5 * time.Second).UnixMilli(), Data: []byte("item1")}
	item2 := Item{TTL: now.Add(20 * time.Second).UnixMilli(), Data: []byte("item2")}

	_ = ds.CreateItemSync(item1)
	_ = ds.CreateItemSync(item2)

	// Should retrieve only item1 if we limit to 10 seconds
	items, err := ds.ItemsSized(10)
	assert.NoError(t, err)
	assert.Len(t, items, 1)
	assert.Equal(t, "item1", string(items[0].Data))
}

func TestCreateAck(t *testing.T) {
	ds := setupDB(t)
	id := time.Now().UnixMilli()
	err := ds.CreateAck(id)
	assert.NoError(t, err)
	exsists, err := ds.CheckAckDeleteExsists(id)
	assert.NoError(t, err)
	assert.True(t, exsists)
}

func TestDeleteAckWithoutSet(t *testing.T) {
	ds := setupDB(t)
	id := time.Now().UnixMilli()
	exsists, err := ds.CheckAckDeleteExsists(id)
	assert.NoError(t, err)
	assert.False(t, exsists)
}

func TestItemsSized2(t *testing.T) {
	ds := setupDB(t)

	now := time.Now()

	item1 := Item{TTL: now.Add(5 * time.Second).UnixMilli(), Data: []byte("item1")}
	item2 := Item{TTL: now.Add(20 * time.Second).UnixMilli(), Data: []byte("item2")}

	_ = ds.CreateItemSync(item1)
	_ = ds.CreateItemSync(item2)

	// Should retrieve only item1 if we limit to 10 seconds
	items, err := ds.ItemsSized(20)
	assert.NoError(t, err)
	assert.Len(t, items, 2)
	assert.Equal(t, "item1", string(items[0].Data))
	assert.Equal(t, "item2", string(items[1].Data))
}

func TestDeleteItemRange(t *testing.T) {
	ds := setupDB(t)

	now := time.Now()
	item1 := Item{TTL: now.Add(5 * time.Second).UnixMilli(), Data: []byte("item1")}
	item2 := Item{TTL: now.Add(10 * time.Second).UnixMilli(), Data: []byte("item2")}
	item3 := Item{TTL: now.Add(15 * time.Second).UnixMilli(), Data: []byte("item3")}

	_ = ds.CreateItemSync(item1)
	_ = ds.CreateItemSync(item2)
	_ = ds.CreateItemSync(item3)

	start := now.Add(5 * time.Second).UnixMilli()
	end := now.Add(10 * time.Second).UnixMilli()

	err := ds.DeleteItemRange(start, end)
	assert.NoError(t, err)

	items, err := ds.ItemsSized(30)
	assert.NoError(t, err)

	assert.Len(t, items, 1) // Only item3 should remain
	assert.Equal(t, "item3", string(items[0].Data))
}

func TestDeleteItemRangeSingleItem(t *testing.T) {
	ds := setupDB(t)

	now := time.Now()
	item1 := Item{TTL: now.Add(5 * time.Second).UnixMilli(), Data: []byte("item1")}

	_ = ds.CreateItemSync(item1)
	// start := now.Add(5 * time.Second).UnixMilli()
	// end := now.Add(15 * time.Second).UnixMilli()
	items, err := ds.ItemsSized(10)
	assert.NoError(t, err)

	err = ds.DeleteItemRange(int64(items[0].TTL), int64(items[len(items)-1].TTL))
	assert.NoError(t, err)

	items, err = ds.ItemsSized(30)
	assert.NoError(t, err)

	assert.Len(t, items, 0)
	// assert.Equal(t, "item3", items[0].Data)
}
