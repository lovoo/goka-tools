package pg

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akrylysov/pogreb"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/storage"
)

type semaphore chan struct{}

type StorageGroup struct {
	sema    semaphore
	logger  logger.Logger
	options *Options
}

// Acquire acquires resources
func (s semaphore) Acquire() {
	s <- struct{}{}
}

// Release releases resources
func (s semaphore) Release() {
	<-s
}

func NewStorageGroup(options *Options, parallel int, logger logger.Logger) *StorageGroup {
	return &StorageGroup{
		sema:    make(semaphore, parallel),
		logger:  logger,
		options: options,
	}
}

func (sg *StorageGroup) Build(path string) (storage.Storage, error) {
	return Build(path, sg.options, sg.sema, sg.logger)
}

type sst struct {
	logger    logger.Logger
	path      string
	sema      semaphore
	offset    int64
	recovered chan struct{}
	opts      *Options
	db        *pogreb.DB
	close     chan struct{}
	closeWg   sync.WaitGroup
	closed    chan struct{}
}

var (
	offsetKey = "__offset"
)

// Build builds an sqlite storage for goka
func Build(path string, options *Options, sema semaphore, logger logger.Logger) (storage.Storage, error) {

	if options == nil {
		options = DefaultOptions()
	}

	sema.Acquire()
	defer sema.Release()
	db, err := pogreb.Open(path, &pogreb.Options{
		BackgroundSyncInterval:       0, // we'll do sync/compact on our own
		BackgroundCompactionInterval: 0, // we'll do sync/compact on our own
	})
	if err != nil {
		log.Fatalf("Error opening pogreb database: %v", err)
		return nil, nil
	}

	return &sst{
		logger:    logger,
		path:      path,
		sema:      sema,
		recovered: make(chan struct{}),
		opts:      options,
		db:        db,
		close:     make(chan struct{}),
		closed:    make(chan struct{}),
	}, nil
}

func newTickerOrNever(duration time.Duration) (<-chan time.Time, func(), func(d time.Duration)) {
	if duration <= 0 {
		return nil, func() {}, func(_ time.Duration) {}
	}

	ticker := time.NewTicker(duration)
	return ticker.C, ticker.Stop, ticker.Reset
}

func (s *sst) Open() error {

	go func() {
		s.closeWg.Add(1)
		defer s.closeWg.Done()

		compactC, compactStop, compactReset := newTickerOrNever(s.opts.Recovery.CompactionInterval)
		defer compactStop()
		syncC, syncStop, syncReset := newTickerOrNever(s.opts.Recovery.BatchedOffsetSync)
		defer syncStop()

		for {
			select {
			case <-s.close:
				return
			case <-s.recovered:
				return
			case <-compactC:
				s.doCompact()
				// reset the timer
				compactReset(s.opts.Recovery.CompactionInterval + s.jitteredDuration(s.opts.Recovery.CompactionInterval))
			case <-syncC:
				if s.offset != 0 {
					s.putOffset(atomic.LoadInt64(&s.offset), true)
				}
				// reset the timer
				syncReset(s.opts.Recovery.BatchedOffsetSync + s.jitteredDuration(s.opts.Recovery.BatchedOffsetSync))
			}
		}
	}()

	go s.compactLoop()

	return nil
}

func (s *sst) doCompact() {
	s.sema.Acquire()
	start := time.Now()
	if _, err := s.db.Compact(); err != nil {
		s.logger.Printf("error compacting %s: %v", s.path, err)
	}
	s.logger.Printf("Compaction %s done (took %.2f seconds)", s.path, time.Since(start).Seconds())
	s.sema.Release()
}

func (s *sst) compactLoop() {
	s.closeWg.Add(1)
	defer s.closeWg.Done()

	compactC, compactStop, compactReset := newTickerOrNever(s.opts.CompactionInterval)
	defer compactStop()
	syncC, syncStop, syncReset := newTickerOrNever(s.opts.SyncInterval)
	defer syncStop()

	for {
		select {
		case <-s.close:
			return

		case <-compactC:

			// skip compaction if we're not recovered yet
			select {
			case <-s.recovered:
			default:
				break
			}

			s.sema.Acquire()
			start := time.Now()
			if _, err := s.db.Compact(); err != nil {
				s.logger.Printf("error compacting: %v", err)
			}
			s.logger.Printf("compacting %s done (took %.2f seconds)", s.path, time.Since(start).Seconds())
			s.sema.Release()
			// reset the timer
			compactReset(s.opts.CompactionInterval + s.jitteredDuration(s.opts.CompactionInterval))
		case <-syncC:
			// skip compaction if we're not recovered yet
			select {
			case <-s.recovered:
			default:
				break
			}

			s.sema.Acquire()
			start := time.Now()
			if err := s.db.Sync(); err != nil {
				s.logger.Printf("error syncing %s: %v", s.path, err)
			}
			s.logger.Printf("Syncing %s done (took %.2f seconds)", s.path, time.Since(start).Seconds())
			s.sema.Release()
			// reset the timer
			syncReset(s.opts.SyncInterval + s.jitteredDuration(s.opts.SyncInterval))
		}
	}
}

func (s *sst) jitteredDuration(duration time.Duration) time.Duration {
	return time.Duration(float64(duration) * (s.opts.JitterMaxFraction * (rand.Float64()*2.0 - 1.0)))
}

func (s *sst) Close() error {
	close(s.close)
	defer close(s.closed)
	s.closeWg.Wait()
	return s.db.Close()
}
func (s *sst) Has(key string) (bool, error) {
	return false, nil
}

func (s *sst) Get(key string) ([]byte, error) {
	return s.db.Get([]byte(key))
}

func (s *sst) Set(key string, value []byte) error {
	return s.db.Put([]byte(key), value)
}

func (s *sst) Delete(key string) error {
	return nil
}

func (s *sst) GetOffset(defValue int64) (int64, error) {

	// if we're recovered, read offset from the storage, otherwise
	// read our local copy, as it is probably newer
	select {
	case <-s.recovered:
	default:

		localOffset := atomic.LoadInt64(&s.offset)
		// if it's 0, it's either really 0 or just has never been loaded from storage,
		// so only return if != 0
		if localOffset != 0 {
			return localOffset, nil
		}
	}

	data, err := s.Get(offsetKey)
	if err != nil {
		return 0, err
	}

	if data == nil {
		return defValue, nil
	}

	value, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error decoding offset: %v", err)
	}

	return value, nil
}

func (s *sst) putOffset(offset int64, sync bool) error {
	err := s.db.Put([]byte(offsetKey), []byte(strconv.FormatInt(offset, 10)))
	if err != nil {
		return err
	}
	if sync {
		return s.db.Sync()
	}
	return nil
}

func (s *sst) SetOffset(offset int64) error {

	atomic.StoreInt64(&s.offset, offset)

	select {
	case <-s.recovered:
		return s.putOffset(offset, s.opts.SyncAfterOffset)
	default:
		if s.opts.Recovery.BatchedOffsetSync == 0 {
			s.putOffset(offset, true)
		}
	}
	return nil
}

func (s *sst) MarkRecovered() error {
	if s.offset != 0 {
		err := s.putOffset(atomic.LoadInt64(&s.offset), true)
		if err != nil {
			return err
		}
	}

	// one last compaction, so we don't end up with a huge task when starting to process
	s.doCompact()

	close(s.recovered)
	return nil
}

func (s *sst) Iterator() (storage.Iterator, error) {
	return &iterator{
		it: s.db.Items(),
	}, nil
}

type iterator struct {
	it *pogreb.ItemIterator

	curKey []byte
	curVal []byte
	curErr error
}

func (i *iterator) Next() bool {
	i.curKey, i.curVal, i.curErr = i.it.Next()
	return i.curErr == nil

}
func (i *iterator) Err() error {
	return i.curErr
}
func (i *iterator) Key() []byte {
	return i.curKey
}
func (i *iterator) Value() ([]byte, error) {
	return i.curVal, i.Err()
}
func (i *iterator) Release() {
}
func (i *iterator) Seek(key []byte) bool {
	return false
}

func (s *sst) IteratorWithRange(start, limit []byte) (storage.Iterator, error) {
	return nil, fmt.Errorf("not implemented")
}
