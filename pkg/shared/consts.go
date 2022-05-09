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
)

func TruePath(path string) string {
	basePath := configs.GetConfig(configs.BaseDirectoryPath)
	return fmt.Sprintf("%s/%s", basePath, path)
}
