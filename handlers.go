package main

import (
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gowerm123/jdb/pkg/jdbql"
)

func jdbHandler(rw http.ResponseWriter, req *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("Fatal -", r)
		}
	}()

	body := readRequestBody(req)
	jdbql.Parse(string(body))

	rw.WriteHeader(400)
}

func readRequestBody(req *http.Request) []byte {
	bytes, err := ioutil.ReadAll(req.Body)

	if err != nil {
		log.Println("error - failed to read request")
		return nil
	}
	return bytes
}
