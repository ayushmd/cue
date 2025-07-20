package cue

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
)

var QPrefix string = "queue"
var QPrefixByte []byte = []byte(QPrefix)
var IPrefix string = "items"
var IPrefixByte []byte = []byte(IPrefix)
var ZPrefix string = "zombie"
var ZPrefixByte []byte = []byte(ZPrefix)
var DLQPrefix string = "dead"
var DLQPrefixByte []byte = []byte(DLQPrefix)
var AckPrefix string = "ack"
var AckPrefixByte []byte = []byte(AckPrefix)
var oldestIEpocByte []byte = []byte(fmt.Sprintf("%s:0000000000000:0000000000000", IPrefix))
var oldestZEpocByte []byte = []byte(fmt.Sprintf("%s:0000000000000:0000000000000", ZPrefix))
var oldestDEpocByte []byte = []byte(fmt.Sprintf("%s:0000000000000", DLQPrefix))

type DataStorage struct {
	db    *pebble.DB
	flag  int32
	zflag int32
}

func (ds *DataStorage) TryLock() bool {
	return atomic.CompareAndSwapInt32(&ds.flag, 0, 1)
}

func (ds *DataStorage) Unlock() {
	atomic.StoreInt32(&ds.flag, 0)
}

func (ds *DataStorage) ZTryLock() bool {
	return atomic.CompareAndSwapInt32(&ds.zflag, 0, 1)
}

func (ds *DataStorage) ZUnlock() {
	atomic.StoreInt32(&ds.zflag, 0)
}

func NewDataStorage(folder string) *DataStorage {
	db, err := pebble.Open(folder, &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
	ds := &DataStorage{
		db: db,
	}
	return ds
}

func (ds *DataStorage) GetQueues() ([]string, error) {
	arr := make([]string, 0)
	it, err := ds.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(fmt.Sprintf("%s:0", QPrefix)),
		UpperBound: []byte(fmt.Sprintf("%s:z", QPrefix)),
	})
	if err != nil {
		return arr, err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		arr = append(arr, string(it.Value()))
	}
	return arr, nil
}

func (ds *DataStorage) CreateQueue(name string) error {
	key := fmt.Sprintf("%s:%s", QPrefix, name)
	return ds.db.Set([]byte(key), []byte(name), pebble.Sync)
}

func (ds *DataStorage) DeleteQueue(name string) error {
	key := fmt.Sprintf("%s:%s", QPrefix, name)
	return ds.db.Delete([]byte(key), pebble.Sync)
}

func (ds *DataStorage) DB() *pebble.DB {
	return ds.db
}

func (ds *DataStorage) CreateItemAsync(item Item) error {
	data, err := item.Encode()
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s:%d:%d", IPrefix, item.TTL, item.Id)
	return ds.db.Set([]byte(key), data, pebble.NoSync)
}

func (ds *DataStorage) CreateItemSync(item Item) error {
	data, err := item.Encode()
	if err != nil {
		return err
	}
	return ds.CreateSync(item.Id, int64(item.TTL), data)
}

func (ds *DataStorage) CreateSync(id int64, ttl int64, value []byte) error {
	key := fmt.Sprintf("%s:%d:%d", IPrefix, ttl, id)
	return ds.db.Set([]byte(key), value, pebble.Sync)
}

func (ds *DataStorage) PeekItem() (Item, error) {
	iter, err := ds.db.NewIter(nil)
	if err != nil {
		return Item{}, err
	}
	defer iter.Close()

	iter.SeekGE(oldestIEpocByte)
	if !iter.Valid() {
		return Item{}, nil
	}

	key := iter.Key()
	if len(key) < len(IPrefix)+1 || !bytes.HasPrefix(key, []byte(IPrefix)) {
		return Item{}, nil
	}

	var item Item
	if err := item.Decode(iter.Value()); err != nil {
		return Item{}, err
	}

	return item, nil
}

func (ds *DataStorage) PeekTTL() (int64, error) {
	iter, err := ds.db.NewIter(nil)
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	iter.SeekGE(oldestIEpocByte)
	if !iter.Valid() {
		return 0, nil
	}

	key := iter.Key()
	if len(key) < 19 || !bytes.HasPrefix(key, IPrefixByte) {
		return 0, nil
	}

	// Extract and parse epoch part from key
	epochBytes := key[len(IPrefix)+1 : len(IPrefix)+14]
	num, err := strconv.ParseInt(string(epochBytes), 10, 64)
	if err != nil {
		return 0, err
	}

	return num, nil
}

func (ds *DataStorage) PeekTTLZombie() (int64, error) {
	iter, err := ds.db.NewIter(nil)
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	iter.SeekGE(oldestZEpocByte)
	if !iter.Valid() {
		return 0, nil
	}

	key := iter.Key()
	if len(key) < 19 || !bytes.HasPrefix(key, ZPrefixByte) {
		return 0, nil
	}

	// Extract and parse epoch part from key
	epochBytes := key[len(ZPrefix)+1 : len(ZPrefix)+14]
	num, err := strconv.ParseInt(string(epochBytes), 10, 64)
	if err != nil {
		return 0, err
	}
	return num, nil
}

func (ds *DataStorage) PeekZombieItem() (Item, error) {
	iter, err := ds.db.NewIter(nil)
	if err != nil {
		return Item{}, err
	}
	defer iter.Close()

	iter.SeekGE(oldestZEpocByte)
	if !iter.Valid() {
		return Item{}, nil
	}

	key := iter.Key()
	if len(key) < len(ZPrefix)+1 || !bytes.HasPrefix(key, []byte(ZPrefix)) {
		return Item{}, nil
	}

	var item Item
	if err := item.Decode(iter.Value()); err != nil {
		return Item{}, err
	}

	return item, nil
}

func (ds *DataStorage) ItemsSized(size int) ([]Item, error) {
	arr := make([]Item, 0)
	curr := time.Now().Add(time.Duration(size) * time.Second).UnixMilli()
	currEncode := fmt.Sprintf("%s:%d", IPrefix, curr)
	it, err := ds.db.NewIter(&pebble.IterOptions{
		LowerBound: oldestIEpocByte,
		UpperBound: []byte(currEncode),
	})
	if err != nil {
		return arr, err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		var item Item
		err := item.Decode(it.Value())
		if err != nil {
			return arr, err
		}
		arr = append(arr, item)
	}
	return arr, nil
}

func (ds *DataStorage) DeleteItemRange(start, end int64) error {
	startK := fmt.Sprintf("%s:%d", IPrefix, start)
	endK := fmt.Sprintf("%s:%d", IPrefix, end+1)
	// it, _ := ds.db.NewIter(&pebble.IterOptions{
	// 	LowerBound: []byte(startK),
	// 	UpperBound: []byte(endK),
	// })
	// for it.First(); it.Valid(); it.Next() {
	// 	fmt.Println("Items in range:", string(it.Key()), string(it.Value()))
	// }
	return ds.db.DeleteRange([]byte(startK), []byte(endK), pebble.Sync)
}

func (ds *DataStorage) DeleteItem(item Item) error {
	startK := fmt.Sprintf("%s:%d:%d", IPrefix, item.TTL, item.Id)
	return ds.db.Delete([]byte(startK), pebble.Sync)
}

func (ds *DataStorage) BatchDeleteItem(b *pebble.Batch, item Item) error {
	startK := fmt.Sprintf("%s:%d:%d", IPrefix, item.TTL, item.Id)
	return b.Delete([]byte(startK), pebble.Sync)
}

func (ds *DataStorage) CreateDead(id int64, data []byte) error {
	deadK := fmt.Sprintf("%s:%d", DLQPrefix, id)
	return ds.db.Set([]byte(deadK), data, pebble.Sync)
}

func (ds *DataStorage) CreateDeadItem(item Item) error {
	data, err := item.Encode()
	if err != nil {
		return err
	}
	return ds.CreateDead(item.Id, data)
}

func (ds *DataStorage) CreateZombie(id int64, ttl int64, data []byte) error {
	zK := fmt.Sprintf("%s:%d:%d", ZPrefix, ttl, id)
	return ds.db.Set([]byte(zK), data, pebble.NoSync)
}

func (ds *DataStorage) NewBatch() *pebble.Batch {
	return ds.db.NewBatch()
}
func (ds *DataStorage) CreateZombieItem(item Item) error {
	data, err := item.Encode()
	if err != nil {
		return err
	}
	return ds.CreateZombie(item.Id, int64(item.TTL), data)
}

// func (ds *DataStorage) SetZombieItem(item Item) error {
// 	zK := fmt.Sprintf("%s:%d:%d", ZPrefix, time.Now().Add(time.Duration(5)*time.Second).UnixMilli(), item.Id)
// 	return ds.db.Set()
// }

func (ds *DataStorage) BatchCreateZombieItem(batch *pebble.Batch, item Item) error {
	data, err := item.Encode()
	if err != nil {
		return err
	}
	zK := fmt.Sprintf("%s:%d:%d", ZPrefix, item.TTL, item.Id)
	return batch.Set([]byte(zK), data, pebble.NoSync)
}

func (ds *DataStorage) BatchDeleteZombieItem(batch *pebble.Batch, item Item) error {
	zK := fmt.Sprintf("%s:%d:%d", ZPrefix, item.TTL, item.Id)
	return batch.Delete([]byte(zK), pebble.NoSync)
}

func (ds *DataStorage) DeleteZombieItem(item Item) error {
	zK := fmt.Sprintf("%s:%d:%d", ZPrefix, item.TTL, item.Id)
	return ds.db.Delete([]byte(zK), pebble.NoSync)
}

func (ds *DataStorage) BatchCreateDeadItem(batch *pebble.Batch, item Item) error {
	data, err := item.Encode()
	if err != nil {
		return err
	}
	zK := fmt.Sprintf("%s:%d", DLQPrefix, item.Id)
	return batch.Set([]byte(zK), data, pebble.NoSync)
}

func (ds *DataStorage) DeleteDeadItem(id int64) error {
	startK := fmt.Sprintf("%s:%d", DLQPrefix, id)
	return ds.db.Delete([]byte(startK), pebble.Sync)
}

func (ds *DataStorage) BatchDeleteDeadItem(b *pebble.Batch, id int64) error {
	startK := fmt.Sprintf("%s:%d", DLQPrefix, id)
	return b.Delete([]byte(startK), pebble.Sync)
}

type cleanupOptions struct {
	remove bool
}

var Clean cleanupOptions = cleanupOptions{remove: true}
var NoClean cleanupOptions = cleanupOptions{remove: false}

// remove tells to clear items
func (ds *DataStorage) CleanupDeadItems(ctx context.Context) (chan Item, error) {
	ch := make(chan Item, 10)
	iter, err := ds.db.NewIter(&pebble.IterOptions{
		LowerBound: oldestDEpocByte,
	})
	if err != nil {
		iter.Close()
		close(ch)
		return ch, err
	}
	go func() {
		defer func() {
			iter.Close()
			close(ch)
		}()

		for iter.First(); iter.Valid(); iter.Next() {
			select {
			case <-ctx.Done():
				iter.Close()
				close(ch)
				return // Cancel the goroutine immediately
			default:
			}

			key := iter.Key()
			if len(key) < len(DLQPrefix)+1 || !bytes.HasPrefix(key, DLQPrefixByte) {
				break
			}
			var item Item
			if err := item.Decode(iter.Value()); err == nil {
				ch <- item
			}
		}
	}()
	return ch, nil
}

func (ds *DataStorage) CreateAck(id int64) error {
	key := fmt.Sprintf("%s:%d", AckPrefix, id)
	return ds.db.Set([]byte(key), []byte("1"), pebble.NoSync)
}

func (ds *DataStorage) CheckAckDeleteExsists(id int64) (exsists bool, err error) {
	key := fmt.Sprintf("%s:%d", AckPrefix, id)
	value, closer, err := ds.db.Get([]byte(key))
	if err != nil {
		if err == pebble.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	if err := closer.Close(); err != nil {
		return false, err
	}
	if string(value) == "1" {
		exsists = true
	}
	err = ds.db.Delete([]byte(key), pebble.NoSync)
	return
}
