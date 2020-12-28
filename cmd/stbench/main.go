package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lovoo/goka-tools/pg"
	"github.com/lovoo/goka-tools/sqlstorage"
	"github.com/lovoo/goka/storage"
	"github.com/spf13/pflag"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	path        = pflag.String("path", "/tmp/sbench", "path to sbench. Defaults to /tmp/sbench-<now>")
	topic       = pflag.String("topic", "topic", "topic to use. Probably no need to change it.")
	partition   = pflag.Int("partition", 0, "partition to use. Probably no need to change it")
	numKeys     = pflag.Int("keys", 5000000, "number of different keys to use")
	nthWrite    = pflag.Int("nth-write", 200, "which nth operation is a write. Default=100, i.e. every 100th operation is a write")
	keyLength   = pflag.Int("key-len", 24, "length of the key in bytes. default is 24")
	valueLength = pflag.Int("val-len", 500, "length of the value in bytes. Default is 500")
	reuseTable  = pflag.Bool("reuse", false, "if set to true, instead of rewriting keys, it will iterate the storage to get all keys")
	clearPath   = pflag.Bool("clear", false, "if set to true, deletes the value passed to 'path' before running.")
	compact     = pflag.Bool("compact", false, "if set to true, compacts the database after loading")
	storageType = pflag.String("storage", "leveldb", "storage to use. defaults to leveldb")
)

var (
	keys    []string
	offset  int64
	opcount int64
)

const (
	keyChars = "0123456789abcdef"
)

func main() {

	pflag.Parse()

	if *clearPath {
		log.Printf("cleaning output path")
		err := os.RemoveAll(*path)
		if err != nil {
			log.Fatalf("Error removing path %s: %v", *path, err)
		}
	}

	// st := createPogrepStorage()
	// st := createSqliteStorage()
	st := createStorage()
	err := st.Open()
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			log.Fatalf("Error closing storage: %v", err)
		} else {
			log.Printf("closed database")
		}
	}()

	if *reuseTable {
		log.Printf("loading all tables")
		it, err := st.Iterator()
		if err != nil {
			log.Fatalf("error creating storage iterator: %v", err)
		}
		for it.Next() {
			if len(keys)%100000 == 0 {
				log.Printf("loaded %d keys", len(keys))
			}
			keys = append(keys, string(it.Key()))
		}
		it.Release()
		if len(keys) == 0 {
			log.Fatalf("database was empty. does not make sense to reuse")
		}
		log.Printf("Read %d keys from database", len(keys))
	} else {

		log.Printf("creating %d keys, and initializing database", *numKeys)
		for i := 0; i < *numKeys; i++ {

			key := makeKey()
			keys = append(keys, key)
			write(st, key)
		}
	}

	head := int(math.Min(30.0, float64(len(keys))))
	log.Printf("first %d keys: %v", head, keys[:head])

	log.Printf("init done, finishing first batch write")
	if err := st.MarkRecovered(); err != nil {
		log.Fatalf("marking storage as recovered failed: %v", err)
	}

	go printStats()

	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-wait:
			log.Printf("stopping")
			return
		default:
		}

		currentOps := atomic.AddInt64(&opcount, 1)

		key := keys[rand.Intn(len(keys))]

		isWrite := currentOps%int64(*nthWrite) == 0

		// we should write
		if isWrite {
			write(st, key)
		} else {
			// we should read
			_, err := st.Get(key)
			if err != nil {
				log.Fatalf("Error reading key: %v", err)
			}
		}

	}
}

func makeKey() string {
	// we're going to transform it to hex, so we only need half of the bytes from rand
	byteLength := (*keyLength) / 2
	val := make([]byte, byteLength)
	n, err := rand.Read(val)
	if n != byteLength || err != nil {
		log.Fatalf("error generating key: %v", err)
	}
	return hex.EncodeToString(val)
}

func write(st storage.Storage, key string) {
	value := make([]byte, *valueLength)
	rand.Read(value)

	st.Set(key, value)
	offset++
	st.SetOffset(offset)
}

func printStats() {
	var (
		lastOpCount int64
		lastStats   = time.Now()
	)
	for {
		time.Sleep(1 * time.Second)
		curOp := atomic.LoadInt64(&opcount)

		now := time.Now()
		opDiff := curOp - lastOpCount
		timeDiff := now.Sub(lastStats)

		// store cur values for next loop
		lastStats = now
		lastOpCount = curOp

		log.Printf("ops per second: %.1f", float64(opDiff)/timeDiff.Seconds())
	}

}

func createPogrepStorage() storage.Storage {
	st, err := pg.Build(*path)
	if err != nil {
		log.Fatalf("error opening database %v", err)
	}

	return st
}

func createSqliteStorage() storage.Storage {
	st, err := sqlstorage.Build(fmt.Sprintf("file:%s/%s", *path, "database.db"))
	if err != nil {
		log.Fatalf("error opening database %v", err)
	}

	return st
}

func createStorage() storage.Storage {
	storageOptions := &opt.Options{

		// AltFilters: []filter.Filter{filter.NewBloomFilter(16)},
		// mongo-Ids have 24hex-values, which storing as string is 192 bits
		// Filter: filter.NewBloomFilter(5),
		// OpenFilesCacheCapacity: ,
		OpenFilesCacher: opt.NoCacher,
		// BlockCacheCapacity:  10 * opt.MiB,
		// WriteBuffer:         50 * opt.MiB,
		// CompactionTableSize: 4 * 1024 * 1024,
		// DisableSeeksCompaction: true,
	}

	fp := filepath.Join(*path, fmt.Sprintf("%s.%d", *topic, *partition))
	log.Printf("Opening storage")
	db, err := leveldb.OpenFile(fp, storageOptions)
	if err != nil {
		log.Fatalf("error opening leveldb: %v", err)
	}

	if *compact {
		log.Printf("starting compaction")
		err = db.CompactRange(util.Range{})
		if err != nil {
			log.Fatalf("error compacting: %v", err)
		}
		log.Printf("compaction done")
	} else {
		log.Printf("skipping compaction")
	}

	st, err := storage.New(db)
	log.Printf("loading storage done")
	if err != nil {
		log.Fatalf("error creating storage: %v", err)
	}

	go func() {
		var (
			lastStats = new(leveldb.DBStats)
			lastTime  = time.Now()
		)
		for {
			time.Sleep(1 * time.Second)
			var curStats leveldb.DBStats
			err := db.Stats(&curStats)
			if err != nil {
				log.Fatalf("Error getting db stats: %v", err)
			}
			now := time.Now()

			diffSecs := now.Sub(lastTime).Seconds()
			log.Printf("io (mb/s): r=%.1f, w=%.1f", float64(curStats.IORead-lastStats.IORead)/diffSecs/(1024*1024), float64(curStats.IOWrite-lastStats.IOWrite)/diffSecs/(1024*1024))
			log.Printf("level durations %+v", curStats.LevelDurations)

			lastTime = now
			lastStats = &curStats

		}
	}()

	return st
}
