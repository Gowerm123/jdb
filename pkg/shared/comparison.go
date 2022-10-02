package shared

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"reflect"
)

type PredicatePayload struct {
	Field, Comparator string
	Target            interface{}
}

func InterrogateSchema(filename string) (Schema, error) {
	file, err := os.OpenFile(filename, 0644, fs.FileMode(os.O_RDONLY))
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(file)
	var line []byte
	for line, _, err = reader.ReadLine(); string(line) == "\n"; {
		if err != nil {
			return nil, err
		}
	}
	var object map[string]interface{}
	err = json.Unmarshal(line, &object)
	if err != nil {
		return nil, err
	}

	for k, v := range object {
		object[k] = InterrogateType(v)
	}

	return object, nil
}

func InterrogateType(object interface{}) string {
	switch object.(type) {
	case int, uint, uint8, uint16, uint32, uint64, int32, int64:
		return JsonInt
	case float64, float32:
		return JsonFloat
	case string:
		return JsonString
	case bool:
		return JsonBool
	case []any:
		return JsonList
	case map[string]any:
		return JsonMap
	default:

		panic(fmt.Sprintf("unknown type for object %v in is %s", object, fmt.Sprint(reflect.TypeOf(object))))
	}
}

func Compare(value, predicateTarget interface{}, comparator string) bool {
	valueType, targetType := InterrogateType(value), InterrogateType(predicateTarget)
	if valueType != targetType {
		panic("value type needs to be same as target type for comparison")
	}

	switch valueType {
	case JsonBool:
		return compareBools(value.(bool), predicateTarget.(bool), comparator)
	case JsonFloat:
		return compareFloats(value.(float64), predicateTarget.(float64), comparator)
	case JsonInt:
		return compareInts(value.(int), predicateTarget.(int), comparator)
	case JsonString:
		return compareStrings(value.(string), predicateTarget.(string), comparator)
	}
	return false
}

func compareInts(value, otherValue int, comparator string) bool {
	switch comparator {
	case ">":
		return value > otherValue
	case "<":
		return value < otherValue
	case "=":
		return value == otherValue
	case "!=":
		return value != otherValue
	case "<=":
		return value <= otherValue
	case ">=":
		return value >= otherValue
	}
	return false
}

func compareBools(value, otherValue bool, comparator string) bool {
	switch comparator {
	case "=":
		return value == otherValue
	case "!=":
		return value != otherValue
	}
	return false
}

func compareStrings(value, otherValue string, comparator string) bool {
	switch comparator {
	case ">":
		return value > otherValue
	case "<":
		return value < otherValue
	case "=":
		return value == otherValue
	case "!=":
		return value != otherValue
	case "<=":
		return value <= otherValue
	case ">=":
		return value >= otherValue
	}
	return false
}

func compareFloats(value, otherValue float64, comparator string) bool {
	switch comparator {
	case ">":
		return value > otherValue
	case "<":
		return value < otherValue
	case "=":
		return value == otherValue
	case "!=":
		return value != otherValue
	case "<=":
		return value <= otherValue
	case ">=":
		return value >= otherValue
	}
	return false
}

func compareChars(value, otherValue byte, comparator string) bool {
	switch comparator {
	case ">":
		return value > otherValue
	case "<":
		return value < otherValue
	case "=":
		return value == otherValue
	case "!=":
		return value != otherValue
	case "<=":
		return value <= otherValue
	case ">=":
		return value >= otherValue
	}
	return false
}
