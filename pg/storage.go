package pg

import (
	"fmt"
	"log"
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

func NewStorageGroup(options *Options, parallel int) *StorageGroup {
	return &StorageGroup{
		sema: make(semaphore, parallel),
	}
}

func (sg *StorageGroup) Build(path string) (storage.Storage, error) {
	return Build(path, sg.options, sg.sema)
}

type sst struct {
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
func Build(path string, options *Options, sema semaphore) (storage.Storage, error) {

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
		path:      path,
		sema:      sema,
		recovered: make(chan struct{}),
		opts:      options,
		db:        db,
		close:     make(chan struct{}),
		closed:    make(chan struct{}),
	}, nil
}

func (s *sst) Open() error {

	if s.opts.Recovery.BatchedOffsetSync > 0 {
		return nil
	}

	go func() {

		syncTicker := time.NewTicker(s.opts.Recovery.BatchedOffsetSync)
		defer syncTicker.Stop()

		for {
			select {
			case <-s.close:
				return
			case <-s.recovered:
				return
			case <-syncTicker.C:
				s.putOffset(atomic.LoadInt64(&s.offset), true)
			}
		}
	}()

	go s.compactLoop()

	return nil
}

// never interval == 100 years, so basically never
const never = time.Hour * 24 * 365 * 100

func (s *sst) compactLoop() {
	s.closeWg.Add(1)
	defer s.closeWg.Done()

	compactInterval := s.opts.CompactionInterval
	syncInterval := s.opts.SyncInterval

	if compactInterval <= 0 {
		compactInterval = never
	}

	if syncInterval <= 0 {
		syncInterval = never
	}

	compactTicker := time.NewTicker(s.opts.CompactionInterval)
	syncTicker := time.NewTicker(s.opts.SyncInterval)
	for {
		select {
		case <-s.close:
			return

		case <-compactTicker.C:

			// skip compaction if we're not recovered yet
			select {
			case <-s.recovered:
			default:
				break
			}

			s.sema.Acquire()
			start := time.Now()
			logger.Default().Printf("start compacting %s", s.path)
			if _, err := s.db.Compact(); err != nil {
				logger.Default().Printf("error compacting: %v", err)
			}
			logger.Default().Printf("compacting %s done (took %.2f seconds)", s.path, time.Since(start).Seconds())
			s.sema.Release()
			// reset the timer
			compactTicker.Reset(compactInterval)
		case <-syncTicker.C:
			// skip compaction if we're not recovered yet,
			// this will be done in the recovery-sync-worker
			select {
			case <-s.recovered:
			default:
				break
			}

			s.sema.Acquire()
			start := time.Now()
			logger.Default().Printf("start syncing %s", s.path)
			if err := s.db.Sync(); err != nil {
				logger.Default().Printf("error syncing: %v", err)
			}
			logger.Default().Printf("syncing %s done (took %.2f seconds)", s.path, time.Since(start).Seconds())
			s.sema.Release()
			// reset the timer
			syncTicker.Reset(syncInterval)
		}
	}
}

func (s *sst) Close() error {
	close(s.close)
	defer close(s.closed)
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

	select {
	case <-s.recovered:
		return s.putOffset(offset, s.opts.SyncAfterOffset)
	default:
		if s.opts.Recovery.BatchedOffsetSync == 0 {
			s.putOffset(offset, true)
		} else {
			atomic.StoreInt64(&s.offset, offset)
		}
	}
	return nil
}

func (s *sst) MarkRecovered() error {
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
