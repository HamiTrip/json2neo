package json2neo

const (
	L_HAS_NESTED  = "JN_HAS_NESTED"
	L_ROOT_NODE   = "JN_ROOT_NODE"
	L_OBJ_PROP    = "JN_OBJ_PROP"
	L_ARR_PROP    = "JN_ARR_PROP"
	TYPE_ARRAY    = "array"
	TYPE_OBJECT   = "object"
	VAR_STUB      = "stub"
	VAR_ROOT      = "root"
	ROOT_NAME_KEY = "jn_name"
	TYPE_KEY      = "jn_type"
	DATA_KEY      = "jn_data"
	LABELS_KEY    = "jn_labels"
)

var TypeToLabel = map[string]string{
	TYPE_ARRAY:  L_ARR_PROP,
	TYPE_OBJECT: L_OBJ_PROP,
	L_ARR_PROP:  TYPE_ARRAY,
	L_OBJ_PROP:  TYPE_OBJECT,
}
