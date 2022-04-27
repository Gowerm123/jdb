package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/gowerm123/jdb/pkg/database"
	"github.com/gowerm123/jdb/pkg/jdbql"
)

func jdbHandler(rw http.ResponseWriter, req *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("Fatal -", r)
		}
	}()

	body := readRequestBody(req)
	jdbql.AssignParserActives(req, rw)
	jdbql.Parse(string(body))

	rw.WriteHeader(200)
}

func readRequestBody(req *http.Request) []byte {
	bytes, err := ioutil.ReadAll(req.Body)

	if err != nil {
		log.Println("error - failed to read request")
		return nil
	}
	return bytes
}

func UIHandler(rw http.ResponseWriter, req *http.Request) {
	html, _ := ioutil.ReadFile("index.html")

	var bleh string

	tables := database.ListTables()
	for _, table := range tables {
		if table.EntryName == "" {
			continue
		}
		body := fmt.Sprintf("SELECT * FROM %s", table.EntryName)

		reader := strings.NewReader(body)
		resp, err := http.Post("http://127.0.0.1:8142/jdb", "application/json", reader)
		if err != nil {
			log.Println(err)
		}

		response, _ := ioutil.ReadAll(resp.Body)
		bleh += fmt.Sprintf("TABLE - %s<br>", table.EntryName)
		jsonSchema, _ := json.Marshal(table.EntrySchema)
		bleh += fmt.Sprintf("SCHEMA - %s<br>", jsonSchema)
		bleh += fmt.Sprintf("RECORDS<br>")

		var blobs []database.Blob
		json.Unmarshal(response, &blobs)

		for _, blob := range blobs {
			str, _ := json.Marshal(blob)
			bleh += string(str) + "<br>"
		}

		bleh += "<br><br>"
	}

	final := fmt.Sprintf(string(html), bleh)

	rw.Write([]byte(final))
}
