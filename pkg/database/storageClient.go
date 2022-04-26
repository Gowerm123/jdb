package database

type TableEntry struct {
	EntryName        string   `json:"name"`
	EntryDir         string   `json:"directory"`
	EntrySchema      Schema   `json:"schema"`
	PartitionColumns []string `json:"partitionColumns"`
}

type Query struct {
	Target     string
	Columns    []string
	Predicates []Predicate
}

func NewTableEntry(name, dir string, schema Schema, partitionColumns []string) TableEntry {
	return TableEntry{
		EntryName:        name,
		EntryDir:         dir,
		EntrySchema:      schema,
		PartitionColumns: partitionColumns,
	}
}

type StorageClient interface {
	SaveTable(string, Schema, []string) error
	LoadTables() map[string]TableEntry
	DropTable(string) error
	InsertValues(string, []Blob) error
	SelectValues(Query) ([]Blob, error)
}

func ResolveClient() StorageClient {
	return &LocalStorageClient{}
}
