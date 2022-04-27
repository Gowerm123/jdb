package database

type TableEntry struct {
	EntryName        string            `json:"name"`
	EntrySchema      Schema            `json:"schema"`
	PartitionColumns []string          `json:"partitionColumns"`
	Metadata         map[string]string `json:"metadata"`
}

type Query struct {
	Target     string
	Columns    []string
	Predicates []Predicate
}

func NewTableEntry(name string, schema Schema, partitionColumns []string, metadata map[string]string) TableEntry {
	return TableEntry{
		EntryName:        name,
		EntrySchema:      schema,
		PartitionColumns: partitionColumns,
		Metadata:         metadata,
	}
}

type StorageClient interface {
	SaveTable(string, Schema, []string) error
	LoadTables()
	DropTable(string) error
	InsertValues(string, []Blob) error
	SelectValues(Query) ([]Blob, error)
	GetTables() map[string]TableEntry
}

func ResolveClient(isTestEnvironment bool) StorageClient {
	if isTestEnvironment {
		return &testStorageClient{
			tables: make(map[string]TableEntry),
			blobs:  make(map[string][]Blob),
		}
	} else {
		return &LocalStorageClient{}
	}
}
