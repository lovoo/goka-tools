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
	updates     = pflag.Int("updates", 1, "proportion of the number of keys (from all keys), whose value will be updated.")
	reads       = pflag.Int("reads", 10, "proportion of the number of keys (from all keys), that will be read.")
	inserts     = pflag.Int("inserts", 1, "proportion of new keys that will be inserted after recovery.")
	duration    = pflag.Int("duration", 60, "duration in seconds to run operations")
	iterate     = pflag.Bool("iterate", true, "iterate once through the whole storage.")
	keyLength   = pflag.Int("key-len", 24, "length of the key in bytes. default is 24")
	valueLength = pflag.Int("val-len", 500, "length of the value in bytes. Default is 500")
	recovery    = pflag.Bool("recovery", false, "if set to true, simluates recovery by inserting each key once")
	clearPath   = pflag.Bool("clear", false, "if set to true, deletes the value passed to 'path' before running.")
	storageType = pflag.String("storage", "leveldb", "storage to use. defaults to leveldb")
	noHeader    = pflag.Bool("no-header", false, "if set to true, the stats won't print the header")
	statsFile   = pflag.String("stats", "", "file to stats")
)

var (
	keys    []string
	opcount int64
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

	os.MkdirAll(*path, os.ModePerm)

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

	bm := newMetrics(ctx, *path)

	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		bm.writeStats(ctx)
	}()

	defer func() {
		<-statsDone
	}()

	recover(ctx, bm)

	run(ctx, bm)
	cancel()
}

func createAndOpenStorage(bm *benchmetrics, phase string) storage.Storage {
	bm.setState(phase)
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
	var st storage.Storage
	blockWithContext(ctx, "opening storage", func() error {
		st = createAndOpenStorage(bm, "open")
		return nil
	})
	bm.setState("recover")

	log.Printf("creating %d keys, and initializing database", *numKeys)
	for i := 0; i < *numKeys; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		key := makeKey()
		keys = append(keys, key)
		bm.insert()
		writeValue(st, key)
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

type opType int

const (
	read opType = iota
	update
	insert
)

func run(ctx context.Context, bm *benchmetrics) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	var st storage.Storage
	blockWithContext(ctx, "opening storage", func() error {
		st = createAndOpenStorage(bm, "reopen")
		return nil
	})

	bm.setState("commit")
	blockWithContext(ctx, "marking storage recovered", st.MarkRecovered)
	defer blockWithContext(ctx, "closing storage", func() error {
		bm.setState("close")
		return st.Close()
	})

	bm.setState("prepare-run")
	runnerOps := append(append(createOps(ctx, update, *updates),
		createOps(ctx, read, *reads)...),
		createOps(ctx, insert, *inserts)...,
	)
	rand.Shuffle(len(runnerOps), func(i, j int) {
		runnerOps[i], runnerOps[j] = runnerOps[j], runnerOps[i]
	})

	bm.setState("running")

	ctx, cancel := context.WithTimeout(ctx, time.Duration(*duration)*time.Second)
	defer cancel()

	var idx int

runLoop:
	for {
		select {
		case <-ctx.Done():
			fmt.Println()
			break runLoop
		default:
		}

		op := runnerOps[idx%len(runnerOps)]
		switch op {
		case read:
			bm.read()
			key := keys[rand.Intn(len(keys))]
			_, err := st.Get(key)
			if err != nil {
				log.Fatalf("Error reading key: %v", err)
			}
		case update:
			bm.update()
			writeValue(st, makeKey())
		case insert:
			bm.insert()
			key := keys[rand.Intn(len(keys))]
			writeValue(st, key)
		default:
			panic("unsupported operation")
		}
		idx++
	}

	if *iterate {
		it, err := st.Iterator()
		if err != nil {
			log.Fatalf("error creating iterator: %v", err)
		}
		defer it.Release()

		bm.setState("iterate")
		for it.Next() {
			_ = it.Key()
			_, _ = it.Value()
		}
	}
}

func createOps(ctx context.Context, opt opType, number int) []opType {
	ops := make([]opType, 0, number*10)

	for i := 0; i < number*10; i++ {
		ops = append(ops, opt)
	}
	return ops
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

func writeValue(st storage.Storage, key string) {
	value := make([]byte, *valueLength)
	rand.Read(value)

	st.Set(key, value)
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
	state     string
	updates   int64
	inserts   int64
	reads     int64
	reg       metrics.Registry
	diskusage int64
	files     int64
}

func newMetrics(ctx context.Context, path string) *benchmetrics {
	reg := metrics.NewRegistry()

	bm := &benchmetrics{
		reg: reg,
	}

	metrics.RegisterRuntimeMemStats(reg)

	go metrics.CaptureRuntimeMemStats(reg, 3*time.Second)

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:

				var size int64
				var files int64
				err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					if !info.IsDir() {
						size += info.Size()
						files++
					}
					return nil
				})
				if err != nil {
					log.Printf("error getting file size/files: %v", err)
				}

				atomic.StoreInt64(&bm.files, files)
				// disk-usage is blocks*blocksize  - free-blocks*block-size
				atomic.StoreInt64(&bm.diskusage, size)

				ticker.Reset(2 * time.Second)
			}
		}
	}()
	return bm
}

func (bm *benchmetrics) setState(state string) {
	log.Printf("entering state %s", state)
	bm.state = state
}

func (bm *benchmetrics) update() {
	atomic.AddInt64(&bm.updates, 1)
}
func (bm *benchmetrics) insert() {
	atomic.AddInt64(&bm.inserts, 1)
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

	fmt.Fprintf(out, "time,state,reads,updates,inserts,total,mem,disk,files\n")

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
		updates := atomic.SwapInt64(&bm.updates, 0)
		inserts := atomic.SwapInt64(&bm.inserts, 0)

		secDiff := now.Sub(last).Seconds()

		readsPerSecond := float64(reads) / secDiff
		updatesPerSecond := float64(updates) / secDiff
		insertsPerSecond := float64(inserts) / secDiff
		opsPerSecond := float64(reads+updates+inserts) / secDiff

		keyMapSize := int64(len(keys) * (*keyLength))
		mem := bm.reg.Get("runtime.MemStats.HeapAlloc").(metrics.Gauge).Value() - keyMapSize
		// memory calculation has not started yet
		if mem < 0 {
			mem = 0.0
		}

		fmt.Fprintf(out, "%04d,%s,%.0f,%.0f,%.0f,%.0f,%d,%d,%d\n",
			int(time.Since(start).Seconds()),
			bm.state,
			readsPerSecond,
			updatesPerSecond,
			insertsPerSecond,
			opsPerSecond,
			mem,
			bm.diskusage,
			bm.files,
		)

	}
}
