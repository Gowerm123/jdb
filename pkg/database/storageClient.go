package database

import "github.com/gowerm123/jdb/pkg/shared"

type StorageClient interface {
	SaveTable(string, shared.Schema, []string) error
	LoadTables()
	DropTable(string) error
	InsertValues(string, []shared.Blob) error
	SelectValues(shared.Query) ([]shared.Blob, error)
	GetTables() map[string]shared.TableEntry
}

func ResolveClient(isTestEnvironment bool) StorageClient {
	if isTestEnvironment {
		return &testStorageClient{
			tables: make(map[string]shared.TableEntry),
			blobs:  make(map[string][]shared.Blob),
		}
	} else {
		return &LocalStorageClient{}
	}
}
