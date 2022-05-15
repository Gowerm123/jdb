package shared

type Schema map[string]interface{}

type Blob map[string]interface{}

func (sch *Schema) Validate(blobs ...Blob) bool {
	for _, blob := range blobs {
		if !sch.validate(blob) {
			return false
		}
	}

	return true
}

func (sch *Schema) validate(blob Blob) bool {
	for fieldName, fieldType := range *sch {
		if object, ok := blob[fieldName]; !ok || (!sch.checkType(fieldName, object, fieldType)) {

			return false
		}
	}

	return len(blob) == len(*sch)
}

func (sch *Schema) checkType(fieldName string, object, fieldType interface{}) bool {
	if _, ok := fieldType.(string); !ok {
		fieldMap := object.(map[string]interface{})
		fieldSchema := (*sch)[fieldName].(map[string]interface{})
		for field, value := range fieldMap {
			if !sch.checkType(field, value, fieldSchema[field]) {
				return false
			}
		}

		return true
	}

	switch fieldType.(string) {
	case JsonBool:
		if _, ok := object.(bool); !ok {
			return false
		}
		break
	case JsonString:
		if _, ok := object.(string); !ok {
			return false
		}
		break
	case JsonInt:
		if _, ok := object.(int); !ok {
			//ints can deserialize as float64
			if _, ok := object.(float64); !ok {
				return false
			}
		}
		break
	case JsonFloat:
		if _, ok := object.(float64); !ok {
			return false
		}
		break
	}

	return true
}
