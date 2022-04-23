package jdbql

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gowerm123/jdb/pkg/database"
)

const (
	jdbSelect = "SELECT"
	jdbCreate = "CREATE"
	jdbDrop   = "DROP"
	jdbInsert = "INSERT"
	jdbInto   = "INTO"
	jdbValues = "VALUES"
	jdbDelete = "DELETE"
	jdbFrom   = "FROM"
	jdbTable  = "TABLE"
	jdbWhere  = "WHERE"
	jdbIdent  = "ident"
	jdbAs     = "AS"
)

var (
	words       []string
	rawContents string
	truePtr     int
	iterPtr     int
	_cmd        Command
	tokenBuffer []string
	tagBuffer   []Tag

	activeRequest *http.Request
	activeWriter  http.ResponseWriter

	keyWords    []string = []string{jdbSelect, jdbCreate, jdbDrop, jdbInsert, jdbDelete, jdbFrom, jdbTable, jdbAs, jdbInto, jdbValues}
	comparators []string = []string{">", "<", "=", "!=", "<=", ">="}
)

func AssignParserActives(req *http.Request, rw http.ResponseWriter) {
	activeRequest = req
	activeWriter = rw
}

func Parse(command string) {
	reset()
	words = strings.Split(command, " ")
	rawContents = command

	accept()
}

func accept() {
	if iterPtr == len(words) {
		if len(tokenBuffer) > 0 {
			_cmd.addInstruction(parseFromTokenBuffer())
		}

		context := CreateContext(activeRequest, activeWriter, _cmd)
		err := context.Execute()
		if err != nil {
			panic(err)
		}
		return
	}
	switch words[iterPtr] {
	case jdbSelect:
		addToTokenBuffer(jdbSelect)
		nextToken()
		optional()
		expect(jdbFrom)
		break
	case jdbFrom:
		nextToken()
		ident()
		accept()
		break
	case jdbCreate:
		addToTokenBuffer(jdbCreate)
		nextToken()
		expect(jdbTable)
		break
	case jdbTable:
		nextToken()
		ident()
		accept()
		break
	case jdbAs:
		nextToken()
		schema := schema()
		tagBuffer = append(tagBuffer, Tag{key: "schema", value: schema})
		accept()
		break
	case jdbDrop:
		addToTokenBuffer(jdbDrop)
		nextToken()
		expect(jdbTable)
		break
	case jdbInsert:
		addToTokenBuffer(jdbInsert)
		nextToken()
		expect(jdbInto)
		break
	case jdbInto:
		nextToken()
		ident()
		expect(jdbValues)
		break
	case jdbValues:
		nextToken()
		values := values()
		tagBuffer = append(tagBuffer, Tag{key: "values", value: values})
		nextToken()
		accept()
		break
	case jdbWhere:
		nextToken()
		predicate()
		break
	default:
		addToTokenBuffer(words[iterPtr])
		nextToken()
		accept()
		break
	}
}

func expect(command string) {
	if words[iterPtr] != command {
		fatal("unexpected token", words[iterPtr], "expected", command)
	}
	accept()
}

func nextToken() {
	truePtr += len(words[iterPtr]) + 1
	iterPtr++
}

func reset() {
	iterPtr = 0
	truePtr = 0
	_cmd = Command{}
}

func optional() {
	options := ""
	for !isKeyword(words[iterPtr]) {
		options += words[iterPtr]
		nextToken()
	}

	addToTagBuffer("options", options)
}

func ident() {
	addToTokenBuffer(words[iterPtr])
	nextToken()
}

func isKeyword(cmd string) bool {
	return contains(keyWords, cmd)
}

func schema() database.Schema {
	subStr := rawContents[truePtr:]
	var schema database.Schema
	json.Unmarshal([]byte(subStr), &schema)

	return schema
}

func values() []database.Blob {
	blobs := []database.Blob{}

	subStr := rawContents[truePtr:]
	split := strings.Split(subStr, "@@")
	for _, blobStr := range split {
		var blob database.Blob
		json.Unmarshal([]byte(blobStr), &blob)
		blobs = append(blobs, blob)
	}

	return blobs
}

func predicate() {
	field := words[iterPtr]
	nextToken()
	comparator := words[iterPtr]
	nextToken()
	target := words[iterPtr]
	if target[0] == '\'' {
		target = target[1:]
		iterPtr++
		for words[iterPtr][len(words[iterPtr])-1] != '\'' {
			target += " " + words[iterPtr]
		}

		target += " " + words[iterPtr][:len(words[iterPtr])-1]
	}

	_predicate := database.BuildPredicate(field, comparator, target)
	addToTagBuffer("predicate", _predicate)

	nextToken()
	accept()
}

func addToTokenBuffer(str string) {
	tokenBuffer = append(tokenBuffer, str)
}

func parseFromTokenBuffer() Instruction {
	inst := Instruction{
		operation: tokenBuffer[0],
		target:    tokenBuffer[1],
	}

	for _, tag := range tagBuffer {
		inst.addTag(tag)
	}
	tagBuffer = []Tag{}
	tokenBuffer = []string{}
	return inst
}

func fatal(msgs ...string) {
	str := fmt.Sprint(msgs)

	panic(errors.New(str))
}

func contains(ls []string, tr string) bool {
	for _, lsTr := range ls {
		if tr == lsTr {
			return true
		}
	}
	return false
}

func addToTagBuffer(key string, value interface{}) {
	tagBuffer = append(tagBuffer, Tag{
		key:   key,
		value: value,
	})
}
