package sqlstorage

import (
	"database/sql"
	"fmt"

	storage "github.com/lovoo/goka/storage"

	_ "github.com/mattn/go-sqlite3"
)

type sst struct {
	db  *sql.DB
	get *sql.Stmt
	set *sql.Stmt
}

// Build builds an sqlite storage for goka
func Build(path string) (storage.Storage, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("Error opening database: %v", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("Cannot ping database: %v", err)
	}

	getStmt, err := db.Prepare("select value from kv where key=?")
	if err != nil {
		return nil, fmt.Errorf("error preparing get statement: %v")
	}
	setStmt, err := db.Prepare("insert into kv values (?,?)")
	if err != nil {
		return nil, fmt.Errorf("error preparing set statement: %v")
	}

	return &sst{
		db:  db,
		get: getStmt,
		set: setStmt,
	}, nil
}

func (s *sst) Open() error {
	_, err := s.db.Exec(`CREATE TABLE if not exists kv (
		key blob NOT NULL PRIMARY KEY,
		value blob
	 );`)
	return err
}

func (s *sst) Close() error {
	return s.db.Close()
}
func (s *sst) Has(key string) (bool, error) {
	return false, nil
}

func (s *sst) Get(key string) ([]byte, error) {
	var value []byte
	row := s.get.QueryRow(key)

	err := row.Scan(&value)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	return value, err
}

func (s *sst) Set(key string, value []byte) error {
	_, err := s.set.Exec([]byte(key), value)
	return err
}

func (s *sst) Delete(key string) error {
	return nil
}

func (s *sst) GetOffset(def int64) (int64, error) {
	return 0, nil
}

func (s *sst) SetOffset(offset int64) error {
	return nil
}

func (s *sst) MarkRecovered() error {
	return nil
}

func (s *sst) Iterator() (storage.Iterator, error) {
	return nil, nil
}

func (s *sst) IteratorWithRange(start, limit []byte) (storage.Iterator, error) {
	return nil, nil
}
