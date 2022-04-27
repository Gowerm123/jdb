package database

import (
	"fmt"
)

type testStorageClient struct {
	tables map[string]TableEntry
	blobs  map[string][]Blob
}

type TableNotFoundError struct {
	table string
}

func ErrTableNotFound(name string) TableNotFoundError {
	return TableNotFoundError{table: name}
}

func (tnfe TableNotFoundError) Error() string {
	panic(fmt.Sprintf("table %s not found", tnfe.table))
}

func (sc *testStorageClient) DropTable(str string) error {
	delete(sc.tables, str)
	delete(sc.blobs, str)

	return nil
}

func (sc *testStorageClient) InsertValues(target string, values []Blob) error {
	if _, ok := sc.tables[target]; !ok {
		return ErrTableNotFound(target)
	}
	sc.blobs[target] = append(sc.blobs[target], values...)

	return nil
}

func (sc *testStorageClient) LoadTables() {
	sc.tables = make(map[string]TableEntry)
}

func (sc *testStorageClient) SaveTable(name string, schema Schema, partitionColumns []string) error {
	sc.tables[name] = NewTableEntry(name, schema, partitionColumns, nil)
	return nil
}

func (sc *testStorageClient) SelectValues(query Query) ([]Blob, error) {
	return sc.blobs[query.Target], nil
}

func (sc *testStorageClient) GetTables() map[string]TableEntry {
	return sc.tables
}
