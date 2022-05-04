package database

import "github.com/gowerm123/jdb/pkg/shared"

type TableEntry struct {
	EntryName        string            `json:"name"`
	EntrySchema      shared.Schema     `json:"schema"`
	PartitionColumns []string          `json:"partitionColumns"`
	Metadata         map[string]string `json:"metadata"`
}

type Query struct {
	Targets     []string
	Columns     []string
	Predicates  []Predicate
	JoinColumns [][]string
}

func NewTableEntry(name string, schema shared.Schema, partitionColumns []string, metadata map[string]string) TableEntry {
	return TableEntry{
		EntryName:        name,
		EntrySchema:      schema,
		PartitionColumns: partitionColumns,
		Metadata:         metadata,
	}
}

type StorageClient interface {
	SaveTable(string, shared.Schema, []string) error
	LoadTables()
	DropTable(string) error
	InsertValues(string, []shared.Blob) error
	SelectValues(Query) ([]shared.Blob, error)
	GetTables() map[string]TableEntry
}

func ResolveClient(isTestEnvironment bool) StorageClient {
	if isTestEnvironment {
		return &testStorageClient{
			tables: make(map[string]TableEntry),
			blobs:  make(map[string][]shared.Blob),
		}
	} else {
		return &LocalStorageClient{}
	}
}
