package main

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"math/rand"
	"sync"
	"testing"
	"time"
)

var (
	db *pebble.DB
)

func Init() {
	var err error
	cache := pebble.NewCache(1 << 30)
	defer cache.Unref()
	opts := &pebble.Options{
		Cache:                       cache,
		Comparer:                    mvccComparer,
		DisableWAL:                  false,
		FormatMajorVersion:          pebble.FormatNewest,
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               64 << 20, // 64 MB
		Levels:                      make([]pebble.LevelOptions, 7),
		MaxConcurrentCompactions:    3,
		MaxOpenFiles:                16384,
		MemTableSize:                64 << 20,
		MemTableStopWritesThreshold: 4,
		Merger:                      fauxMVCCMerger,
	}

	db, err = pebble.Open("/mnt_10g", opts)
	if err != nil {
		panic(err)
	}
}
func getInstance() *pebble.DB {
	return db
}

type testPebbleDB struct{}

func New() *testPebbleDB {
	return &testPebbleDB{}
}

func (p *testPebbleDB) Set(key, value string) (err error) {
	if err := getInstance().Set([]byte(key), []byte(value), pebble.Sync); err != nil {
		return err
	}
	return nil
}

func (p *testPebbleDB) Get(key string) (v string, err error) {
	value, closer, err := getInstance().Get([]byte(key))
	if err != nil {
		return "", err
	}
	v = string(value)
	if err = closer.Close(); err != nil {
		return "", err
	}
	return v, nil
}

func (p *testPebbleDB) GetMultiWgKey(num int) (tc time.Duration) {
	startT := time.Now()
	var wg sync.WaitGroup
	var i int
	for i = 0; i < num; i++ {
		k, _ := createKeyAndValue(256)
		wg.Add(1)
		go func() {
			_, err := p.Get(k)
			//fmt.Printf("get key %s ,value %s\n", k, v)
			if err != nil {
				//fmt.Printf("get %s err\n", k)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	tc = time.Since(startT)
	fmt.Printf("i : %d \n", i)
	fmt.Printf("number : %d\n", num)
	fmt.Println(tc)
	return tc
}

func createKeyAndValue(size int) (k, v string) {
	//key := 1 + rand.Int()
	//k = strconv.Itoa(key)
	var err error
	key := make([]byte, 512)
	_, err = rand.Read(key)
	if err != nil {
		return
	}
	b := make([]byte, size)
	_, err = rand.Read(b)
	if err != nil {
		return
	}
	return string(key), string(b)
}

func Test_TestOptions(t *testing.T) {
	var err error
	Init()
	localDB := New()
	startT := time.Now()
	for i := 0; i < 1000000; i++ {
		k, v := createKeyAndValue(256)
		//fmt.Printf("set key = %s\n", k)
		err = localDB.Set(k, v)
		if err != nil {
			fmt.Printf("%v", err)
		}
		//fmt.Println(i)
	}
	tc := time.Since(startT)
	fmt.Println(tc)
	localDB.GetMultiWgKey(100)
	localDB.GetMultiWgKey(1000)
	localDB.GetMultiWgKey(10000)
}
