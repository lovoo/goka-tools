package pg

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/akrylysov/pogreb"
	"github.com/lovoo/goka/storage"
)

type sst struct {
	offset              int64
	recovered           chan struct{}
	opts                *Options
	db                  *pogreb.DB
	close               chan struct{}
	closed              chan struct{}
	cancelRecoverSyncer context.CancelFunc
}

var (
	offsetKey = "__offset"
)

// Build builds an sqlite storage for goka
func Build(path string, options *Options) (storage.Storage, error) {
	if options == nil {
		options = DefaultOptions()
	}
	db, err := pogreb.Open(path, &pogreb.Options{

		BackgroundSyncInterval:       options.SyncInterval,
		BackgroundCompactionInterval: options.CompactionInterval,
	})
	if err != nil {
		log.Fatalf("Error opening pogrep database: %v", err)
		return nil, nil
	}

	return &sst{
		recovered: make(chan struct{}),
		opts:      options,
		db:        db,
		close:     make(chan struct{}),
		closed:    make(chan struct{}),
	}, nil
}

func (s *sst) Open() error {
	var ctx context.Context
	ctx, s.cancelRecoverSyncer = context.WithCancel(context.Background())

	go func() {
		defer s.cancelRecoverSyncer()

		syncChan, stopper := newNullableTicker(s.opts.Recovery.BatchedOffsetSync)
		defer stopper()

		for {
			select {
			case <-ctx.Done():
				return
			case <-s.recovered:
				return
			case <-syncChan:
				s.putOffset(atomic.LoadInt64(&s.offset), true)
			}
		}
	}()

	return nil
}

func newNullableTicker(d time.Duration) (<-chan time.Time, func()) {
	if d > 0 {
		t := time.NewTicker(d)
		return t.C, t.Stop
	}
	return nil, func() {}
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
