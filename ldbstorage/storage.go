package ldbstorage

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/storage"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	recoveryCommitOffsetInterval = 30 * time.Second
)

type ldbstorage struct {
	path      string
	logger    logger.Logger
	offset    int64
	recovered chan struct{}
	close     chan struct{}
	closeWg   sync.WaitGroup
	closed    chan struct{}
	// store is the active store, either db or tx
	db *leveldb.DB
}

// New creates a new Storage backed by LevelDB.
func New(path string, logger logger.Logger) (storage.Storage, error) {

	storageOptions := &opt.Options{

		Filter: filter.NewBloomFilter(10),
		// mongo-Ids have 24hex-values, which storing as string is 192 bits
		// Filter: filter.NewBloomFilter(5),
		// OpenFilesCacheCapacity: ,
		// OpenFilesCacher: opt.NoCacher,
		// BlockCacheCapacity:  10 * opt.MiB,
		// WriteBuffer:         50 * opt.MiB,
		// CompactionTableSize: 4 * 1024 * 1024,
	}

	db, err := leveldb.OpenFile(path, storageOptions)

	if err != nil {
		return nil, fmt.Errorf("error opening leveldb: %v", err)
	}

	return &ldbstorage{
		path:      path,
		db:        db,
		logger:    logger,
		recovered: make(chan struct{}),
		close:     make(chan struct{}),
		closed:    make(chan struct{}),
	}, nil
}

func (s *ldbstorage) Open() error {
	go func() {
		syncTicker := time.NewTicker(recoveryCommitOffsetInterval)
		defer syncTicker.Stop()

		for {
			select {
			case <-s.close:
				return
			case <-s.recovered:
				return
			case <-syncTicker.C:
				s.putOffset(atomic.LoadInt64(&s.offset))
			}
		}
	}()

	go func() {
		statsTicker := time.NewTicker(10 * time.Second)
		defer statsTicker.Stop()

		for {
			select {
			case <-s.close:
				return
			case <-statsTicker.C:
				if strings.HasSuffix(s.path, "table.0") {
					var stats leveldb.DBStats

					s.db.Stats(&stats)
					s.logger.Printf("%#v", stats)
				}
			}
		}
	}()

	return nil
}

func (s *ldbstorage) Close() error {
	close(s.close)
	defer close(s.closed)
	s.closeWg.Wait()
	return s.db.Close()
}

// Iterator returns an iterator that traverses over a snapshot of the storage.
func (s *ldbstorage) Iterator() (storage.Iterator, error) {
	snap, err := s.db.GetSnapshot()
	if err != nil {
		return nil, err
	}

	return &iterator{
		iter: s.db.NewIterator(nil, nil),
		snap: snap,
	}, nil
}

// Iterator returns an iterator that traverses over a snapshot of the storage.
func (s *ldbstorage) IteratorWithRange(start, limit []byte) (storage.Iterator, error) {
	snap, err := s.db.GetSnapshot()
	if err != nil {
		return nil, err
	}

	if limit != nil && len(limit) > 0 {
		return &iterator{
			iter: s.db.NewIterator(&util.Range{Start: start, Limit: limit}, nil),
			snap: snap,
		}, nil
	}
	return &iterator{
		iter: s.db.NewIterator(util.BytesPrefix(start), nil),
		snap: snap,
	}, nil

}

func (s *ldbstorage) Has(key string) (bool, error) {
	return s.db.Has([]byte(key), nil)
}

func (s *ldbstorage) Get(key string) ([]byte, error) {
	value, err := s.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("error getting from leveldb (key %s): %v", key, err)
	}
	return value, nil
}

func (s *ldbstorage) GetOffset(defValue int64) (int64, error) {
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

func (s *ldbstorage) Set(key string, value []byte) error {
	if err := s.db.Put([]byte(key), value, nil); err != nil {
		return fmt.Errorf("error setting to leveldb (key %s): %v", key, err)
	}
	return nil
}

func (s *ldbstorage) putOffset(offset int64) error {
	return s.Set(offsetKey, []byte(strconv.FormatInt(offset, 10)))
}

func (s *ldbstorage) SetOffset(offset int64) error {

	select {
	case <-s.recovered:
		return s.putOffset(offset)
	default:
		atomic.StoreInt64(&s.offset, offset)
	}
	return nil
}

func (s *ldbstorage) Delete(key string) error {
	if err := s.db.Delete([]byte(key), nil); err != nil {
		return fmt.Errorf("error deleting from leveldb (key %s): %v", key, err)
	}

	return nil
}

func (s *ldbstorage) MarkRecovered() error {
	err := s.putOffset(atomic.LoadInt64(&s.offset))
	if err != nil {
		return err
	}
	close(s.recovered)
	return nil
}
