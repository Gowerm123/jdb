package database

import (
	"fmt"

	"github.com/gowerm123/jdb/pkg/shared"
)

type testStorageClient struct {
	tables map[string]shared.TableEntry
	blobs  map[string][]shared.Blob
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

func (sc *testStorageClient) InsertValues(target string, values []shared.Blob) error {
	if _, ok := sc.tables[target]; !ok {
		return ErrTableNotFound(target)
	}
	sc.blobs[target] = append(sc.blobs[target], values...)

	return nil
}

func (sc *testStorageClient) LoadTables() {
	sc.tables = make(map[string]shared.TableEntry)
}

func (sc *testStorageClient) SaveTable(name string, schema shared.Schema, partitionColumns []string) error {
	sc.tables[name] = shared.NewTableEntry(name, schema, partitionColumns, nil)
	return nil
}

func (sc *testStorageClient) SelectValues(query shared.Query) ([]shared.Blob, error) {
	return sc.blobs[query.Targets[0]], nil
}

func (sc *testStorageClient) GetTables() map[string]shared.TableEntry {
	return sc.tables
}

func (sc *testStorageClient) ResolveFile(table string) string {
	return fmt.Sprintf("%s/alpha", shared.TruePath(table))
}
