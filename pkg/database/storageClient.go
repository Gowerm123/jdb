package database

type TableEntry struct {
	EntryName   string `json:"name"`
	EntryDir    string `json:"directory"`
	EntrySchema Schema `json:"schema"`
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
}

func ResolveClient() StorageClient {
	return &LocalStorageClient{}
}
