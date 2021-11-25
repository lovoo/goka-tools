package telly

import (
	"context"
	"fmt"

	rdb "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

// DropOffsets deletes the offsets document to make telly start from scratch
func (t *Telly) DropOffsets() error {
	_, err := t.metaTable().Get(t.offsetKey()).Delete().RunWrite(t.Executor())
	return err
}

func createDbAndTable(ctx context.Context, session rdb.QueryExecutor, dbName, tableName string, primaryKey string) error {
	var dbs []string
	if err := rdb.DBList().ReadAll(&dbs, session); err != nil {
		return fmt.Errorf("error listing databases:%v", err)
	}
	var foundDB bool
	for _, db := range dbs {
		if db == dbName {
			foundDB = true
			break
		}
	}
	if !foundDB {
		if err := rdb.DBCreate(dbName).Exec(session); err != nil {
			return fmt.Errorf("error creating database: %v", err)
		}
	}
	var tables []string
	if err := rdb.DB(dbName).TableList().ReadAll(&tables, session); err != nil {
		return fmt.Errorf("error listing tables: %v", err)
	}

	for _, table := range tables {
		if table == tableName {
			if primaryKey != "" {
				var tableConfig map[string]interface{}
				if err := rdb.DB(dbName).Table(tableName).Config().ReadOne(&tableConfig, session); err != nil {
					return fmt.Errorf("error reading table config %s: %v", tableName, err)
				}
				if tableConfig["primary_key"] != primaryKey {
					return fmt.Errorf("table %s is configured with primary key %s, but we need %s. Delete the table first", tableName, tableConfig["primary_key"], primaryKey)
				}
			}
			return nil
		}
	}
	createOpts := rdb.TableCreateOpts{}
	if primaryKey != "" {
		createOpts.PrimaryKey = primaryKey
	}
	if err := rdb.DB(dbName).TableCreate(tableName, createOpts).Exec(session); err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	return nil
}

func (t *Telly) createSecondaryIndex(name string) error {
	var indexes []string
	if err := t.Table().IndexList().ReadAll(&indexes, t.rsess); err != nil {
		return err
	}
	for _, index := range indexes {
		if index == name {
			return nil
		}
	}
	_, err := t.Table().IndexCreate(name).RunWrite(t.rsess)
	return err
}
