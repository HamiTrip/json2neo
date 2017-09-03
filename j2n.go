package json2neo

import (
	"strings"
	"fmt"
	"sync"
)

/*
//NEO orm?! ke json ham dare!?
//Neo connectoro ioc (dependency injection) begire! ye interface ke

//TODO:: har node bayad unique id khodesho dashte bashe ta baraye update ha va ... estefade beshe , masalan Assign dar TSP!! (inheritance)
//TODO:: yebare ye query model e json ham bezanam ke query bezane?? :-/

//TODO:: ham-level haro async bezane!!
*/


type J2N interface {
	SetStubNode(node_id int) J2N
	SetRootLabel(sl string) J2N
	SetRootName(n string) J2N
	Insert(data interface{}) (id int)
	Submit(data interface{}) (id int) // mese haman


	appendToCypher(cypher_part string)

	cypherGenerator()
	/*
	createRootArray(label, name, parent_var string, properties []interface{}) (node_var string) // mitunan hamun nested creatora besazanesh!
	createRootObject(label, name, parent_var string, properties map[string]interface{}) (node_var string)

	create_nested_array_of_array_parent(parent_var string, properties []interface{}) (node_var string)
	create_nested_array_of_object_parent(parent_var, name string, properties []interface{}) (node_var string)

	create_nested_object_of_object_parent(parent_var, name string, properties map[string]interface{}) (node_var string)
	create_nested_object_of_array_parent(parent_var string, properties map[string]interface{}) (node_var string)
	*/
}

type j2n struct {
	sync.Mutex
	data        interface{}
	data_array  []interface{}
	data_object map[string]interface{}
	data_type   string
	cypher      string
	root_name   string
	root_label  string
	root_var    string
	has_stub    bool
}

func (this *j2n)SetStubNode(node_id int) J2N {
	var cypher string = fmt.Sprintf("MATCH (stub) WHERE ID(stub) = %d\n", node_id)
	this.appendToCypher(cypher)
	this.has_stub = true
	return this
}

func (this *j2n)SetRootLabel(rl string) J2N {
	this.root_label = fmt.Sprintf(":%s", strings.ToUpper(rl))
	return this
}

func (this *j2n)SetRootName(n string) J2N {
	this.root_name = n
	return this
}

func (this *j2n)Submit(data interface{}) (id int) {
	return this.Insert(data)
}

func (this *j2n)Insert(data interface{}) (id int) {
	this.data = data
	switch data.(type) {
	case []interface{}:
		this.data_type = TYPE_ARRAY
	case map[string]interface{}:
		this.data_type = TYPE_OBJECT
	default:
		panic(fmt.Sprintf("Only '[]interface{}' and 'map[string]interface{}' are accepted, given: '%T'", data))
	}

	this.cypherGenerator()

	fmt.Println(this.cypher)

	return 0
}

func (this *j2n)appendToCypher(cypher_part string) {
	if strings.TrimSpace(cypher_part) != "" {
		this.Lock()
		this.cypher += fmt.Sprintf(" %s ", cypher_part)
		this.Unlock()
	}
}

func (this *j2n)cypherGenerator() {
	//CREATE (root:JN_CONFIG:JN_OBJ_PROP:JN_ROOT_NODE {jn_name:"config"})
	var type_label string
	switch this.data_type {
	case TYPE_ARRAY:
		type_label = L_ARR_PROP
	case TYPE_OBJECT:
		type_label = L_OBJ_PROP
	}



	var cypher string = fmt.Sprintf(
		"CREATE %s(root%s:%s:%s {%s})\n",
		this.getStubCypherPart(),
		this.root_label,
		type_label,
		L_ROOT_NODE,
		this.getRootNameCypherPart())

	this.appendToCypher(cypher)

}

func (this *j2n)getStubCypherPart() (c string) {
	if this.has_stub {
		c = fmt.Sprintf("(%s)-[%s {%s:'%s'}]->", VAR_STUB, this.root_label, ROOT_NAME_KEY, this.root_name)
	}
	return
}

func (this *j2n)getRootNameCypherPart() (c string) {
	if strings.TrimSpace(this.root_name) != "" {
		c = fmt.Sprintf("%s:'%s'",
			ROOT_NAME_KEY,
			this.root_name)
	}
	return
}

func (this *j2n)getFieldsCypherPart(data) {

}

func NewJ2N() J2N {
	return new(j2n)
}

