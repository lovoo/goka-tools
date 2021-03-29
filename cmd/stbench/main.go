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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lovoo/goka-tools/pg"
	"github.com/lovoo/goka-tools/sqlstorage"
	"github.com/lovoo/goka/storage"
	"github.com/rcrowley/go-metrics"
	"github.com/spf13/pflag"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	path        = pflag.String("path", "/tmp/sbench", "path to sbench. Defaults to /tmp/sbench-<now>")
	topic       = pflag.String("topic", "topic", "topic to use. Probably no need to change it.")
	partition   = pflag.Int("partition", 0, "partition to use. Probably no need to change it")
	numKeys     = pflag.Int("keys", 5000000, "number of different keys to use")
	nthWrite    = pflag.Int("nth-write", 10, "which nth operation is a write. Default=100, i.e. every 100th operation is a write")
	keyLength   = pflag.Int("key-len", 24, "length of the key in bytes. default is 24")
	valueLength = pflag.Int("val-len", 500, "length of the value in bytes. Default is 500")
	recovery    = pflag.Bool("recovery", false, "if set to true, simluates recovery by inserting each key once")
	clearPath   = pflag.Bool("clear", false, "if set to true, deletes the value passed to 'path' before running.")
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

func main() {

	pflag.Parse()

	os.MkdirAll(*path, os.ModeDir)

	if *clearPath {
		log.Printf("cleaning output path")
		err := os.RemoveAll(*path)
		if err != nil {
			log.Fatalf("Error removing path %s: %v", *path, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// cancel when after a signal from terminal
	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-wait:
			log.Printf("---- terminating due to interrupt ----")
			cancel()
		}
	}()

	bm := newMetrics(ctx)

	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		bm.writeStats(ctx)
	}()

	defer func() {
		<-statsDone
	}()

	recover(ctx, bm)

	// cancel the context after configured duration.
	// We have to start the timer here, not using context.WithTimeout, because we want the timeout to be applied
	// only for the "run"-part
	time.AfterFunc(time.Duration(*duration)*time.Second, func() {
		cancel()
	})

	run(ctx, bm)
}

func createAndOpenStorage(bm *benchmetrics) storage.Storage {
	bm.setState("open")
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

	return st
}

func recover(ctx context.Context, bm *benchmetrics) {
	bm.setState("recover")

	st := createAndOpenStorage(bm)

	log.Printf("creating %d keys, and initializing database", *numKeys)
	for i := 0; i < *numKeys; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		key := makeKey()
		keys = append(keys, key)
		bm.write()
		write(st, key)
	}

	bm.setState("commit")
	blockWithContext(ctx, "marking storage recovered", st.MarkRecovered)
	blockWithContext(ctx, "closing storage", func() error {
		bm.setState("close")
		return st.Close()
	})
}

func blockWithContext(ctx context.Context, msg string, blocker func() error) {

	log.Printf("%s", msg)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := blocker(); err != nil {
			log.Fatalf("... error: %v", err)
		}
	}()

	select {
	case <-ctx.Done():
		log.Printf("... aborted by context cancelled")
	case <-done:
		log.Printf("... done")
	}
}

func run(ctx context.Context, bm *benchmetrics) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	bm.setState("prepare")
	var st storage.Storage
	blockWithContext(ctx, "opening storage", func() error {
		st = createAndOpenStorage(bm)
		return nil
	})

	blockWithContext(ctx, "marking storage recovered", st.MarkRecovered)
	defer blockWithContext(ctx, "closing storage", func() error {
		bm.setState("close")
		return st.Close()
	})

	bm.setState("running")

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

		// we should write
		if isWrite {
			bm.write()
			write(st, key)
		} else {
			bm.read()
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
		log.Fatalf("error generating kssdfsdfey: %v", err)
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

	st, err := storage.New(db)
	log.Printf("loading storage done")
	if err != nil {
		log.Fatalf("error creating storage: %v", err)
	}

	return st
}

type benchmetrics struct {
	state  string
	writes int64
	reads  int64
	reg    metrics.Registry
}

func newMetrics(ctx context.Context) *benchmetrics {
	reg := metrics.NewRegistry()

	bm := &benchmetrics{
		reg: reg,
	}

	metrics.RegisterRuntimeMemStats(reg)

	go metrics.CaptureRuntimeMemStats(reg, 3*time.Second)
	return bm
}

func (bm *benchmetrics) setState(state string) {
	log.Printf("entering state %s", state)
	bm.state = state
}

func (bm *benchmetrics) write() {
	atomic.AddInt64(&bm.writes, 1)
}

func (bm *benchmetrics) read() {
	atomic.AddInt64(&bm.reads, 1)
}

func (bm *benchmetrics) writeStats(ctx context.Context) {

	start := time.Now()

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

	fmt.Fprintf(out, "time,state,reads,writes,total,mem\n")

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var (
		last = time.Now()
	)
	for {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}

		now := time.Now()
		reads := atomic.SwapInt64(&bm.reads, 0)
		writes := atomic.SwapInt64(&bm.writes, 0)

		secDiff := now.Sub(last).Seconds()

		readsPerSecond := float64(reads) / secDiff
		writesPerSecond := float64(writes) / secDiff
		opsPerSecond := float64(reads+writes) / secDiff

		keyMapSize := int64(len(keys) * (*keyLength))
		mem := bm.reg.Get("runtime.MemStats.HeapAlloc").(metrics.Gauge).Value() - keyMapSize
		// memory calculation has not started yet
		if mem < 0 {
			mem = 0.0
		}

		fmt.Fprintf(out, "%04d,%s,%.0f,%.0f,%.0f,%d\n",
			int(time.Since(start).Seconds()),
			bm.state,
			readsPerSecond,
			writesPerSecond,
			opsPerSecond,
			mem)

	}
}
