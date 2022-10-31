package shared

import (
	"fmt"

	"github.com/gowerm123/jdb/pkg/configs"
)

const (
	JdbSelect      = "SELECT"
	JdbCreate      = "CREATE"
	JdbDrop        = "DROP"
	JdbInsert      = "INSERT"
	JdbInto        = "INTO"
	JdbValues      = "VALUES"
	JdbDelete      = "DELETE"
	JdbFrom        = "FROM"
	JdbTable       = "TABLE"
	JdbWhere       = "WHERE"
	JdbIdent       = "ident"
	JdbAs          = "AS"
	JdbPartitioned = "PARTITIONED"
	JdbOn          = "ON"
	JdbList        = "LIST"
	JdbGroup       = "GROUP"
	JdbBy          = "BY"
	JdbLoad        = "LOAD"
	JdbDescribe    = "DESCRIBE"
	JsonString     = "string"
	JsonInt        = "int"
	JsonFloat      = "float"
	JsonBool       = "boolean"
	JsonList       = "List"
	JsonMap        = "Map"

	GROUP_BY_COLUMNS  = "group-by-columns"
	LOAD_TARGETS      = "load-targets"
	PARTITION_COLUMNS = "partition-columns"
	SELECT_COLUMNS    = "select-columns"
)

var (
	Operations = []string{JdbSelect, JdbCreate, JdbDrop, JdbInsert, JdbList, JdbDescribe}
)

func TruePath(path string) string {
	basePath := configs.GetConfig(configs.BaseDirectoryPath)
	return fmt.Sprintf("%s/%s", basePath, path)
}
