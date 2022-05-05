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
	Predicates  []Predicate
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

type Predicate struct {
	field      string
	comparator string
	target     interface{}
}

func (pr *Predicate) GetFields() (string, string, interface{}) {
	return pr.field, pr.comparator, pr.target
}

func BuildPredicate(field, comparator string, target interface{}) Predicate {
	return Predicate{
		field:      field,
		comparator: comparator,
		target:     target,
	}
}
