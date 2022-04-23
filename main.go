package main

import (
	"net/http"

	"github.com/go-zoo/bone"
)

func main() {
	mux := bone.New()

	mux.Post("/jdb", http.HandlerFunc(jdbHandler))

	mux.Get("/ui", http.HandlerFunc(UIHandler))

	http.ListenAndServe(":8142", mux)
}
