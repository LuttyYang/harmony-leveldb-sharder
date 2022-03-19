package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"io/ioutil"
	"kv-compare/compare"
	"net/http"
	_ "net/http/pprof"
	"path/filepath"
	"sync"
	"time"
)

const (
	safePointKey = "kv-compare-safe-point"
)

var shardIdxKey = []byte("__DB_SHARED_INDEX__")

func buildMultiDB(savePath string, diskCount int, diskShards int) *compare.MultiDBInstance {
	var err error
	var dbs = make([]compare.DBInstance, diskCount*diskShards)

	// clean when error
	defer func() {
		if err != nil {
			for _, db := range dbs {
				if db != nil {
					_ = db.Close()
				}
			}
		}
	}()

	// async open
	wg := sync.WaitGroup{}
	for i := 0; i < diskCount; i++ {
		for j := 0; j < diskShards; j++ {
			shardPath := filepath.Join(savePath, fmt.Sprintf("disk%02d", i), fmt.Sprintf("block%02d", j))
			dbIndex := i*diskShards + j
			wg.Add(1)
			go func() {
				defer wg.Done()

				ldb, err := compare.NewLevelDBInstance(shardPath)
				if err != nil {
					panic(err)
				}

				indexByte := make([]byte, 8)
				binary.BigEndian.PutUint64(indexByte, uint64(dbIndex))
				inDBIndex, getErr := ldb.Get(shardIdxKey)
				if getErr != nil {
					if getErr == leveldb.ErrNotFound {
						putErr := ldb.Add(shardIdxKey, indexByte)
						if putErr != nil {
							err = putErr
							return
						}
					} else {
						err = getErr
						return
					}
				} else if bytes.Compare(indexByte, inDBIndex) != 0 {
					err = fmt.Errorf("db shard index error, need %v, got %v", indexByte, inDBIndex)
					return
				}

				dbs[dbIndex] = ldb
			}()
		}
	}

	wg.Wait()

	return compare.NewMultiDBInstance(dbs)
}

func main() {
	from := flag.String("from", "", "from dir")
	to := flag.String("to", "", "to dir")

	flag.Parse()

	go func() {
		http.ListenAndServe(":8649", nil)
	}()

	instance, err := compare.NewLevelDBInstance(*from)
	if err != nil {
		panic(err)
	}
	defer instance.Close()

	kvInstance := buildMultiDB(*to, 8, 4)
	kvCompare := compare.NewCompare(instance, kvInstance, 32)

	// load point
	file, err := ioutil.ReadFile(safePointKey)
	if err == nil {
		err = kvCompare.LoadSafePoint(file)
		if err != nil {
			panic(err)
		}
	}

	savePoint := func() {
		point, err := kvCompare.SaveSafePointAndGet()
		if err != nil {
			panic(err)
		}

		kvCompare.PrintProcess()

		err = ioutil.WriteFile(safePointKey, point, 0644)
		if err != nil {
			panic(err)
		}
	}

	// save point
	go func() {
		for tick := range time.Tick(time.Second * 10) {
			if time.Now().Sub(tick) < 5*time.Second {
				savePoint()
			}
		}
	}()
	kvCompare.Start()
	savePoint()
}
