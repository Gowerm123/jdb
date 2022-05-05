package main

import (
	"log"
	"net/http"
	"strconv"

	"github.com/go-zoo/bone"
	"github.com/gowerm123/jdb/pkg/configs"
	"github.com/gowerm123/jdb/pkg/database"
	"github.com/gowerm123/jdb/pkg/server"
)

func main() {
	configs.Load()

	database.InitClient(false)

	mux := bone.New()

	mux.Post("/jdb", http.HandlerFunc(jdbHandler))

	mux.Get("/ui", http.HandlerFunc(UIHandler))

	numWorkersConfig := configs.GetConfig("server.worker.count")
	numWorkersInt, err := strconv.Atoi(numWorkersConfig)
	if err != nil {
		log.Fatal("configuration server.worker.count must be a valid integer")
	}

	server.StartWorkers(numWorkersInt)

	http.ListenAndServe(":8142", mux)
}
