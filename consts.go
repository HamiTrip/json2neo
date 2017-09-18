package json2neo

const (
	LabelHasNested = "JN_HAS_NESTED"
	LabelRootNode = "JN_ROOT_NODE"
	LabelObjProp = "JN_OBJ_PROP"
	LabelArrProp = "JN_ARR_PROP"
	TypeArray = "array"
	TypeObject = "object"
	VarStub = "stub"
	VarRoot = "root"
	IdKey = "_id"
	RootNameKey = "jn_name"
	TypeKey = "jn_type"
	DataKey = "jn_data"
	LabelsKey = "jn_labels"
	ValueTrue = "true"
)

/*
Convert labels to type and vise versa
 */
var TypeToLabel = map[string]string{
	TypeArray:  LabelArrProp,
	TypeObject: LabelObjProp,
	LabelArrProp:  TypeArray,
	LabelObjProp:  TypeObject,
}
