package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble"
)

var QPrefix string = "queue"
var QPrefixByte []byte = []byte(QPrefix)
var IPrefix string = "items"
var IPrefixByte []byte = []byte(IPrefix)
var DLQPrefix string = "dead"
var DLQPrefixByte []byte = []byte(DLQPrefix)

type DataStorage struct {
	db *pebble.DB
}

func NewDataStorage() *DataStorage {
	db, err := pebble.Open("demo", &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
	ds := &DataStorage{
		db: db,
	}
	return ds
}

func (ds *DataStorage) GetQueues() ([][]byte, error) {
	arr := make([][]byte, 0)
	it, err := ds.db.NewIter(nil)
	if err != nil {
		return arr, err
	}
	for it.SeekPrefixGE(QPrefixByte); it.Valid(); it.Next() {
		arr = append(arr, it.Value())
	}
	return arr, nil
}

func (ds *DataStorage) CreateQueue(name string) error {
	key := fmt.Sprintf("%s:%s", QPrefix, name)
	return ds.db.Set([]byte(key), []byte("1"), pebble.Sync)
}

func (ds *DataStorage) DB() *pebble.DB {
	return ds.db
}

func (ds *DataStorage) CreateItemAsync(item Item) error {
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s:%d", IPrefix, item.TTL)
	return ds.db.Set([]byte(key), data, pebble.NoSync)
}

func (ds *DataStorage) CreateItemSync(item Item) error {
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s:%d", IPrefix, item.TTL)
	return ds.db.Set([]byte(key), data, pebble.Sync)
}

func (ds *DataStorage) CreateSync(ttl int64, value []byte) error {
	key := fmt.Sprintf("%s:%d", IPrefix, ttl)
	return ds.db.Set([]byte(key), value, pebble.Sync)
}

var oldestEpoch int64 = time.Unix(0, 0).UnixMilli()
var oldestEpocByte []byte = []byte(fmt.Sprintf("%s:%d", IPrefix, oldestEpoch))

func (ds *DataStorage) PeekItem() (Item, error) {
	it, err := ds.db.NewIter(nil)
	if err != nil {
		return Item{}, err
	}
	it.SeekGE(oldestEpocByte)
	var item Item
	if it.Valid() {
		data := it.Value()
		err := json.Unmarshal(data, &item)
		if err != nil {
			return Item{}, err
		}
		return item, nil
	}
	return Item{}, nil
}

func (ds *DataStorage) PeekTTL() (int64, error) {
	// it, err := ds.db.NewIter(nil)
	iter, err := ds.db.NewIter(nil)
	if err != nil {
		return 0, err
	}
	iter.SeekGE(oldestEpocByte)
	if iter.Valid() {
		fmt.Println("Recieved in peek: ", string(iter.Value()), string(iter.Key()[5:]))
		k := iter.Key()[6:]
		s := string(k)
		num, _ := strconv.ParseInt(s, 10, 64)
		return num, nil
	}
	return 0, nil
}

func (ds *DataStorage) ItemsSized(size int) ([]Item, error) {
	arr := make([]Item, 0)
	curr := time.Now().Add(time.Duration(size) * time.Second).UnixMilli()
	currEncode := fmt.Sprintf("%s:%d", IPrefix, curr)
	it, err := ds.db.NewIter(&pebble.IterOptions{
		LowerBound: oldestEpocByte,
		UpperBound: []byte(currEncode),
	})
	if err != nil {
		return arr, err
	}
	for it.First(); it.Valid(); it.Next() {
		var item Item
		err := json.Unmarshal(it.Value(), &item)
		if err != nil {
			return arr, err
		}
		fmt.Println("The item: ", item)
		arr = append(arr, item)
	}
	return arr, nil
}

func (ds *DataStorage) DeleteItemRange(start, end int64) error {
	startK := fmt.Sprintf("%s:%d", IPrefix, start)
	endK := fmt.Sprintf("%s:%d", IPrefix, end)
	return ds.db.DeleteRange([]byte(startK), []byte(endK), pebble.Sync)
}

// func data() {
// 	db, err := pebble.Open("demo", &pebble.Options{})
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	key := []byte("lorem")
// 	if err := db.Set(key, []byte("ipsum"), pebble.Sync); err != nil {
// 		log.Fatal(err)
// 	}
// 	// if err := db.Merge(key, []byte("hola"), pebble.Sync); err != nil {
// 	// 	log.Fatal(err)
// 	// }
// 	iter, _ := db.NewIter(nil)
// 	for iter.First(); iter.Valid(); iter.Next() {
// 		fmt.Printf("%s %s\n", iter.Key(), string(iter.Value()))
// 	}
// 	if err := iter.Close(); err != nil {
// 		log.Fatal(err)
// 	}
// 	value, closer, err := db.Get([]byte("lorem"))
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	if err := closer.Close(); err != nil {
// 		log.Fatal(err)
// 	}
// 	fmt.Printf("%s %s\n", key, value)
// 	if err := db.Close(); err != nil {
// 		log.Fatal(err)
// 	}
// }
