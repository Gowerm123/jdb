module github.com/gowerm123/jdb

go 1.19

require github.com/go-zoo/bone v1.3.0

replace github.com/gowerm123/jdb/pkg/database => ./pkg/database/

replace github.com/gowerm123/jdb/pkg/jdbql => ./pkg/jdbql/

replace github.com/gowerm123/jdb/pkg/shared => ./pkg/shared/

replace github.com/gowerm123/jdb/pkg/server => ./pkg/server/

replace github.com/gowerm123/jdb/pkg/dbSync => ./pkg/dbSync/