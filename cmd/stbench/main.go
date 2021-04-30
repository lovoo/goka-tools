package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lovoo/goka-tools/pg"
	"github.com/lovoo/goka-tools/sqlstorage"
	"github.com/lovoo/goka/storage"
	"github.com/spf13/pflag"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	path           = pflag.String("path", "/tmp/sbench", "path to sbench. Defaults to /tmp/sbench-<now>")
	topic          = pflag.String("topic", "topic", "topic to use. Probably no need to change it.")
	partition      = pflag.Int("partition", 0, "partition to use. Probably no need to change it")
	numKeys        = pflag.Int("keys", 5000000, "number of different keys to use")
	duration       = pflag.Int("duration", 30, "duration in seconds to run operations of different types")
	iterateTimeout = pflag.Int("iterate-timeout", 10, "duration in seconds to run iteration, or 0 to iterate thorugh the whole storage")
	enableIterate  = pflag.Bool("iterate", true, "iterate once through the whole storage.")
	keyLength      = pflag.Int("key-len", 24, "length of the key in bytes. default is 24")
	valueLength    = pflag.Int("val-len", 500, "length of the value in bytes. Default is 500")
	recovery       = pflag.Bool("recovery", false, "if set to true, simluates recovery by inserting each key once")
	clearPath      = pflag.Bool("clear", false, "if set to true, deletes the value passed to 'path' before running.")
	storageType    = pflag.String("storage", "leveldb", "storage to use. defaults to leveldb")
	noHeader       = pflag.Bool("no-header", false, "if set to true, the stats won't print the header")
	statsFile      = pflag.String("stats", "", "file to stats")
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

	iterate(ctx, bm)

	cancel()
}

func createAndOpenStorage(bm *benchmetrics, phase string) storage.Storage {
	bm.setState(phase)
	var st storage.Storage
	switch *storageType {
	case "leveldb":
		st = createStorage()
	case "pogreb-offsetsync":
		options := pg.DefaultOptions()
		options.Recovery.BatchedOffsetSync = 0
		options.SyncAfterOffset = true
		st = createpogrebStorage(options)
	case "pogreb-batch-recover":
		options := pg.DefaultOptions()

		// completely turn off compaction/sync
		options.CompactionInterval = 0
		options.SyncInterval = 0
		st = createpogrebStorage(options)
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
		s := time.Now()
		writeValue(st, key)
		bm.insert(time.Since(s))
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

	bm.setState("running-read")
	runForTime(ctx, func(idx int) {
		s := time.Now()
		readValue(st, keys[rand.Intn(len(keys))])
		bm.read(time.Since(s))
	})

	bm.setState("running-update")
	runForTime(ctx, func(idx int) {
		s := time.Now()
		writeValue(st, keys[rand.Intn(len(keys))])
		bm.update(time.Since(s))
	})

	bm.setState("running-insert")
	runForTime(ctx, func(idx int) {
		s := time.Now()
		writeValue(st, makeKey())
		bm.insert(time.Since(s))
	})

	bm.setState("running-mixed")
	runForTime(ctx, func(idx int) {
		op := idx % 3
		s := time.Now()
		switch op {
		case 0:
			s := time.Now()
			readValue(st, keys[rand.Intn(len(keys))])
			bm.read(time.Since(s))
		case 1:
			writeValue(st, keys[rand.Intn(len(keys))])
			bm.update(time.Since(s))
		case 2:
			writeValue(st, makeKey())
			bm.insert(time.Since(s))
		}
	})
}

func runForTime(ctx context.Context, do func(idx int)) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(*duration)*time.Second)
	defer cancel()

	var idx int

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		do(idx)

		idx++
	}
}

func iterate(ctx context.Context, bm *benchmetrics) {

	if !*enableIterate {
		return
	}

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

	blockWithContext(ctx, "marking storage recovered", st.MarkRecovered)
	defer blockWithContext(ctx, "closing storage", func() error {
		bm.setState("close")
		return st.Close()
	})

	it, err := st.Iterator()
	if err != nil {
		log.Fatalf("error creating iterator: %v", err)
	}
	defer it.Release()

	if *iterateTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(*iterateTimeout)*time.Second)
		defer cancel()
	}

	bm.setState("iterate")
	s := time.Now()
	for it.Next() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		_ = it.Key()
		_, _ = it.Value()
		bm.read(time.Since(s))
		s = time.Now()
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

	err := st.Set(key, value)
	if err != nil {
		panic(fmt.Errorf("error executing set: %v", err))
	}
}

func readValue(st storage.Storage, key string) {
	_, err := st.Get(key)
	if err != nil {
		panic(fmt.Errorf("Error reading key: %v", err))
	}
}

func createpogrebStorage(options *pg.Options) storage.Storage {
	sg := pg.NewStorageGroup(options, 1)
	st, err := sg.Build(*path)
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
	sync.Mutex

	state           string
	updates         int64
	updateLatencies int64
	inserts         int64
	insertLatencies int64
	reads           int64
	readLatencies   int64
	diskusage       int64
	files           int64
	memory          int64
}

func newMetrics(ctx context.Context, path string) *benchmetrics {
	bm := &benchmetrics{}

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

				mem := readFileValue("/sys/fs/cgroup/memory/memory.usage_in_bytes")
				cache := readFileLineValue("/sys/fs/cgroup/memory/memory.stat", "total_inactive_file")
				bm.memory = mem - cache
				ticker.Reset(2 * time.Second)
			}
		}
	}()
	return bm
}

func readFileValue(file string) int64 {

	memusage, err := ioutil.ReadFile(file)
	if err != nil {
		log.Printf("err reading mem usage: %v", err)
	} else {
		value, err := strconv.ParseInt(strings.TrimSpace(string(memusage)), 10, 64)
		if err != nil {
			log.Printf("error parsing memory usage %s: %v", string(memusage), err)
		}
		return value
	}
	return 0
}

func readFileLineValue(file string, prefix string) int64 {
	contents, err := ioutil.ReadFile(file)
	if err != nil {
		log.Printf("err reading mem usage: %v", err)
		return 0
	}

	for _, line := range strings.Split(string(contents), "\n") {
		if strings.HasPrefix(line, prefix) {
			value, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, prefix)), 10, 64)
			if err != nil {
				log.Printf("error parsing memory usage %s: %v", string(line), err)
			}
			return value
		}
	}
	return 0
}

func (bm *benchmetrics) setState(state string) {
	log.Printf("entering state %s", state)
	bm.state = state
}

func (bm *benchmetrics) update(latency time.Duration) {
	bm.Lock()
	defer bm.Unlock()
	bm.updates++
	bm.updateLatencies += int64(latency)
}
func (bm *benchmetrics) insert(latency time.Duration) {
	bm.Lock()
	defer bm.Unlock()
	bm.inserts++
	bm.insertLatencies += int64(latency)
}

func (bm *benchmetrics) read(latency time.Duration) {
	bm.Lock()
	defer bm.Unlock()
	bm.reads++
	bm.readLatencies += int64(latency)
}

func (bm *benchmetrics) getResetRead() (float64, float64, float64) {
	bm.Lock()
	defer bm.Unlock()
	v, lats := bm.reads, bm.readLatencies
	bm.reads = 0
	bm.readLatencies = 0
	latMean := 0.0
	if v > 0 {
		latMean = time.Duration(lats).Seconds() / float64(v)
	}
	return float64(v), time.Duration(lats).Seconds(), latMean
}
func (bm *benchmetrics) getResetInsert() (float64, float64, float64) {
	bm.Lock()
	defer bm.Unlock()
	v, lats := bm.inserts, bm.insertLatencies
	bm.inserts = 0
	bm.insertLatencies = 0
	latMean := 0.0
	if v > 0 {
		latMean = time.Duration(lats).Seconds() / float64(v)
	}
	return float64(v), time.Duration(lats).Seconds(), latMean
}
func (bm *benchmetrics) getResetUpdate() (float64, float64, float64) {
	bm.Lock()
	defer bm.Unlock()
	v, lats := bm.updates, bm.updateLatencies
	bm.updates = 0
	bm.updateLatencies = 0
	latMean := 0.0
	if v > 0 {
		latMean = time.Duration(lats).Seconds() / float64(v)
	}
	return float64(v), time.Duration(lats).Seconds(), latMean
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

	fmt.Fprintf(out, "time,state,reads,updates,inserts,readLatMean,updateLatMean,insertLatMean,readOnlys,updateOnlys,writeOnlys,totalOps,mem,disk,files\n")

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
		reads, readLatencies, readLatencyMean := bm.getResetRead()
		updates, updateLatencies, updateLatencyMean := bm.getResetUpdate()
		inserts, insertLatencies, insertLatencyMean := bm.getResetInsert()
		secDiff := now.Sub(last).Seconds()
		last = now

		readsPerSecond := reads / secDiff

		readsOnlyPerSecond := 0.0
		if readLatencies > 0 {
			readsOnlyPerSecond = reads * (secDiff / readLatencies)
		}
		updatesPerSecond := updates / secDiff
		updatesOnlyPerSecond := 0.0
		if updateLatencies > 0 {
			updatesOnlyPerSecond = updates * (secDiff / updateLatencies)
		}
		insertsPerSecond := inserts / secDiff
		insertsOnlyPerSecond := 0.0
		if insertLatencies > 0 {
			insertsOnlyPerSecond = inserts * (secDiff / insertLatencies)
		}
		opsPerSecond := (reads + updates + inserts) / secDiff

		bm.Lock()
		fmt.Fprintf(out, "%04d,%s,%.0f,%.0f,%.0f,%f,%f,%f,%.0f,%.0f,%.0f,%.0f,%d,%d,%d\n",
			int(time.Since(start).Seconds()),
			bm.state,
			readsPerSecond,
			updatesPerSecond,
			insertsPerSecond,
			readLatencyMean,
			updateLatencyMean,
			insertLatencyMean,
			readsOnlyPerSecond,
			updatesOnlyPerSecond,
			insertsOnlyPerSecond,
			opsPerSecond,
			bm.memory,
			bm.diskusage,
			bm.files,
		)
		bm.Unlock()

	}
}
