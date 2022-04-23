package database

type TableEntry struct {
	EntryName   string `json:"name"`
	EntryDir    string `json:"directory"`
	EntrySchema Schema `json:"schema"`
}

type Query struct {
	Target     string
	Columns    string
	Predicates []Predicate
}

func NewTableEntry(name, dir string, schema Schema) TableEntry {
	return TableEntry{
		EntryName:   name,
		EntryDir:    dir,
		EntrySchema: schema,
	}
}

type StorageClient interface {
	SaveTable(string, Schema) error
	LoadTables() map[string]TableEntry
	DropTable(string) error
	InsertValues(string, []Blob) error
	SelectValues(Query) ([]Blob, error)
}

func ResolveClient() StorageClient {
	return &LocalStorageClient{}
}
