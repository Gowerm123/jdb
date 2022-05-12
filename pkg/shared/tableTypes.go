package shared

type TableEntry struct {
	EntryName        string            `json:"name"`
	EntrySchema      Schema            `json:"schema"`
	PartitionColumns []string          `json:"partitionColumns"`
	Metadata         map[string]string `json:"metadata"`
}

type Query struct {
	Targets     []string
	Columns     []string
	Predicates  []func(Blob) Blob
	JoinColumns [][]string
}

func NewTableEntry(name string, schema Schema, partitionColumns []string, metadata map[string]string) TableEntry {
	return TableEntry{
		EntryName:        name,
		EntrySchema:      schema,
		PartitionColumns: partitionColumns,
		Metadata:         metadata,
	}
}
