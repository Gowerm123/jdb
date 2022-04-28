package main

import (
	"net/http"

	"github.com/go-zoo/bone"
	"github.com/gowerm123/jdb/pkg/configs"
	"github.com/gowerm123/jdb/pkg/database"
)

func main() {
	configs.Load()

	database.InitClient(false)

	mux := bone.New()

	mux.Post("/jdb", http.HandlerFunc(jdbHandler))

	mux.Get("/ui", http.HandlerFunc(UIHandler))

	http.ListenAndServe(":8142", mux)
}
