package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/lovoo/goka-tools/pg"
	"github.com/lovoo/goka-tools/sqlstorage"
	"github.com/lovoo/goka/storage"
	"github.com/rcrowley/go-metrics"
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
	nthWrite    = pflag.Int("nth-write", 10, "which nth operation is a write. Default=100, i.e. every 100th operation is a write")
	keyLength   = pflag.Int("key-len", 24, "length of the key in bytes. default is 24")
	valueLength = pflag.Int("val-len", 500, "length of the value in bytes. Default is 500")
	reuseTable  = pflag.Bool("reuse", false, "if set to true, instead of rewriting keys, it will iterate the storage to get all keys")
	clearPath   = pflag.Bool("clear", false, "if set to true, deletes the value passed to 'path' before running.")
	compact     = pflag.Bool("compact", false, "if set to true, compacts the database after loading")
	storageType = pflag.String("storage", "leveldb", "storage to use. defaults to leveldb")
	noHeader    = pflag.Bool("no-header", false, "if set to true, the stats won't print the header")
	duration    = pflag.Int("duration", 60, "duration in seconds to run after recovery")
	statsFile   = pflag.String("stats", "", "file to stats")
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

	var st storage.Storage
	switch *storageType {
	case "leveldb":
		st = createStorage()
	case "pogrep":
		st = createPogrepStorage()
	case "sqlite":
		st = createSqliteStorage()
	default:
		log.Fatalf("invalid storage type: %s", *storageType)
	}

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// cancel when after a signal from terminal
	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-wait:
			log.Printf("terminating due to interrupt")
			cancel()
		}
	}()

	bm := newMetrics(ctx)

	recover(ctx, bm, st)

	commit(ctx, bm, st)

	// cancel the context after configured duration.
	// We have to start the timer here, not using context.WithTimeout, because we want the timeout to be applied
	// only for the "run"-part
	time.AfterFunc(time.Duration(*duration)*time.Second, func() {
		cancel()
	})

	log.Printf("starting to run for %d seconds", *duration)
	run(ctx, bm, st)
	log.Printf("..done")
}

func recover(ctx context.Context, bm *benchmetrics, st storage.Storage) {
	start := time.Now()
	defer func() {
		bm.recover = time.Since(start)
	}()

	if *reuseTable {
		log.Printf("loading data from table")
		it, err := st.Iterator()
		defer it.Release()

		select {
		case <-ctx.Done():
			return
		default:
		}
		if err != nil {
			log.Fatalf("error creating storage iterator: %v", err)
		}
		for it.Next() {
			if len(keys)%100000 == 0 {
				log.Printf("loaded %d keys", len(keys))
			}
			keys = append(keys, string(it.Key()))
		}

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
}

func commit(ctx context.Context, bm *benchmetrics, st storage.Storage) {
	start := time.Now()
	defer func() {
		bm.commit = time.Since(start)
	}()

	// ctx is currently not used, because MarkRecovered does not support it.
	log.Printf("committing after recovery")
	if err := st.MarkRecovered(); err != nil {
		log.Fatalf("marking storage as recovered failed: %v", err)
	}
	log.Printf("Commit done")
}

func run(ctx context.Context, bm *benchmetrics, st storage.Storage) {

	var ops int64
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		ops++

		key := keys[rand.Intn(len(keys))]

		isWrite := ops%int64(*nthWrite) == 0

		bm.allOps.Mark(1)
		// we should write
		if isWrite {
			bm.writeOps.Mark(1)
			write(st, key)
		} else {
			bm.readOps.Mark(1)
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
		// OpenFilesCacher: opt.NoCacher,
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

	return st
}

type benchmetrics struct {
	readOps  metrics.Meter
	writeOps metrics.Meter
	allOps   metrics.Meter
	recover  time.Duration
	commit   time.Duration
}

func newMetrics(ctx context.Context) *benchmetrics {
	reg := metrics.NewRegistry()

	bm := &benchmetrics{
		allOps:   metrics.NewRegisteredMeter("ops.all", reg),
		readOps:  metrics.NewRegisteredMeter("ops.read", reg),
		writeOps: metrics.NewRegisteredMeter("ops.write", reg),
	}

	metrics.RegisterRuntimeMemStats(reg)

	go metrics.CaptureRuntimeMemStats(reg, 3*time.Second)

	go writeStats(ctx, bm, reg)
	return bm
}

func writeStats(ctx context.Context, bm *benchmetrics, reg metrics.Registry) {

	var out io.Writer = os.Stdout
	if *statsFile != "" {
		outFile, err := os.Create(*statsFile)
		if err != nil {
			log.Fatalf("Error creating stats file %s: %v", *statsFile, err)
		}
		defer func() {
			if err := outFile.Close(); err != nil {
				log.Printf("error closing stats file: %v", err)
			}
		}()
		out = outFile
	}

	if !*noHeader {
		fmt.Fprintf(out, "storage,numkeys,keylen,vallen,recov,commit,ops.all,ops.read,ops.write,mem\n")
	}

	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	var (
		last         = time.Now()
		lastOpsAll   int64
		lastOpsRead  int64
		lastOpsWrite int64
	)
	for {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}

		recov := bm.recover.Milliseconds()
		commit := bm.commit.Milliseconds()
		now := time.Now()
		opsAllNow := bm.allOps.Count()
		opsReadNow := bm.readOps.Count()
		opsWriteNow := bm.writeOps.Count()
		opsAll := float64(opsAllNow-lastOpsAll) / now.Sub(last).Seconds()
		opsRead := float64(opsReadNow-lastOpsRead) / now.Sub(last).Seconds()
		opsWrite := float64(opsWriteNow-lastOpsWrite) / now.Sub(last).Seconds()
		last = now
		lastOpsAll = opsAllNow
		lastOpsRead = opsReadNow
		lastOpsWrite = opsWriteNow

		keyMapSize := int64(len(keys) * (*keyLength))
		mem := reg.Get("runtime.MemStats.HeapAlloc").(metrics.Gauge).Value() - keyMapSize
		// memory calculation has not started yet
		if mem < 0 {
			continue
		}
		fmt.Fprintf(out, "%s,%d,%d,%d,%d,%d,%.0f,%.0f,%.0f,%d\n",
			*storageType,
			*numKeys,
			*keyLength,
			*valueLength,
			recov, commit, opsAll, opsRead, opsWrite, mem)

	}
}
