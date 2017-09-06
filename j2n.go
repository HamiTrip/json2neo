package json2neo

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"time"
	"hami/ums/base"
)

/*
//NEO orm?! ke json ham dare!?
//Neo connectoro ioc (dependency injection) begire! ye interface ke

//TODO:: har node bayad unique id khodesho dashte bashe ta baraye update ha va ... estefade beshe , masalan Assign dar TSP!! (inheritance)
//TODO:: yebare ye query model e json ham bezanam ke query bezane?? :-/

//TODO:: ham-level haro async bezane!!

//TODO:: support baraye struct!
*/

type J2N interface {
	SetStubNode(node_id int64) J2N
	SetRootLabel(sl string) J2N
	SetRootName(n string) J2N
	SetConn(conn golangNeo4jBoltDriver.Conn) J2N
	Insert(data interface{}) (id int64 , count int)
	Submit(data interface{}) (id int64 , count int)

	execCypher(cypher_part string) interface{}

	cypherGenerator()

	create_nested(node_key interface{}, parent_var, parent_type, node_type, node_var string, properties interface{}, parent_c chan int)
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
	sync.WaitGroup
	total_nodes int
	neo_conn    golangNeo4jBoltDriver.Conn
	has_conn    bool
	data        interface{}
	data_type   string
	cypher      []string
	root_id     int64
	root_name   string
	root_label  string
	has_stub    bool
	stub_cypher string
}

func (this *j2n) SetStubNode(node_id int64) J2N {
	this.stub_cypher , this.has_stub = fmt.Sprintf("MATCH (%s) WHERE ID(%s) = %d\n", VAR_STUB, VAR_STUB, node_id) , true
	return this
}

func (this *j2n) SetConn(conn golangNeo4jBoltDriver.Conn) J2N {
	this.neo_conn, this.has_conn = conn, true
	return this
}

func (this *j2n) SetRootLabel(rl string) J2N {
	//TODO:: panic if it have :! only one label are accepted!
	this.root_label = fmt.Sprintf(":%s", strings.ToUpper(rl))
	return this
}

func (this *j2n) SetRootName(n string) J2N {
	this.root_name = n
	return this
}

func (this *j2n) Submit(data interface{}) (id int64 , count int) {
	return this.Insert(data)
}

func (this *j2n) Insert(data interface{}) (id int64 , count int) {
	fmt.Println("START:", time.Now().Unix())
	if !this.has_conn {
		panic("Neo4j connection not found!")
	}
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
	this.Wait()
	return this.root_id , this.total_nodes
}

func (this *j2n) execCypher(cypher_part string) (res interface{}) {
	if strings.TrimSpace(cypher_part) != "" {
		this.Lock()
		//this.cypher += fmt.Sprintf(" %s ", cypher_part)
		//this.cypher = append(this.cypher, fmt.Sprintf(" %s ", cypher_part))
		//base.Warning("start cypher:", cypher_part)
		result, err := this.neo_conn.QueryNeo(cypher_part, map[string]interface{}{})
		//base.Warning("ended cypher:", cypher_part)
		if err == nil {
			r, _, _ := result.All()
			res = r[0][0]
			result.Close()
			this.total_nodes++
		} else {
			panic(err)
		}
		this.Unlock()
	}
	return
}

func (this *j2n) cypherGenerator() {
	//TODO:: Pipeline!
	var type_label string
	switch this.data_type {
	case TYPE_ARRAY:
		type_label = L_ARR_PROP
	case TYPE_OBJECT:
		type_label = L_OBJ_PROP
	}
	var sfc string
	var c chan int = make(chan int, 1)
	var fc []string = this.getFieldsCypherPart(this.data, VAR_ROOT, this.data_type, c)
	if len(fc) > 0 {
		sfc = "," + strings.Join(fc, ",")
	}
	var cypher string = fmt.Sprintf(
		"%s\n CREATE %s(%s%s:%s:%s {%s%s}) RETURN ID(%s)\n",
		this.stub_cypher,
		this.getStubCypherPart(),
		VAR_ROOT,
		this.root_label,
		type_label,
		L_ROOT_NODE,
		this.getRootNameCypherPart(),
		sfc,
		VAR_ROOT,
	)

	var node_id interface{} = this.execCypher(cypher)
	switch {
	case node_id == nil:
		panic("Cannot create root node!")
	default:
		c <- int(node_id.(int64))
		this.root_id = node_id.(int64)
		base.Warning("root_node_id:", node_id, time.Now().Unix())
	}

}

func (this *j2n) getStubCypherPart() (c string) {
	if this.has_stub {
		c = fmt.Sprintf("(%s)-[%s {%s:'%s'}]->", VAR_STUB, this.root_label, ROOT_NAME_KEY, this.root_name)
	}
	return
}

func (this *j2n) getRootNameCypherPart() (c string) {
	if strings.TrimSpace(this.root_name) != "" {
		c = fmt.Sprintf("%s:'%s'",
			ROOT_NAME_KEY,
			this.root_name)
	}
	return
}

func (this *j2n) create_nested(node_key interface{}, parent_var, parent_type, node_type, node_var string, properties interface{}, parent_c chan int) {
	var node_type_label string
	switch node_type {
	case TYPE_ARRAY:
		node_type_label = L_ARR_PROP
	case TYPE_OBJECT:
		node_type_label = L_OBJ_PROP
	}
	var c chan int = make(chan int, 1)
	var sfc string = strings.Join(this.getFieldsCypherPart(properties, node_var, node_type, c), ",")
	var cName string
	if parent_type == TYPE_OBJECT {
		cName = fmt.Sprintf(",name:'%v'", node_key)
	}
	parent_node_id := <-parent_c
	parent_c <- parent_node_id
	var parent_cypher string = fmt.Sprintf("MATCH (%s) WHERE ID(%s) = %d\n", parent_var, parent_var, parent_node_id)
	var cypher string = fmt.Sprintf(
		"%s \n CREATE (%s)-[:%s {type:'%s'%s}]->(%s:%s {%s}) RETURN ID(%s)\n",
		parent_cypher,
		parent_var,
		L_HAS_NESTED,
		node_type,
		cName,
		node_var,
		node_type_label,
		sfc,
		node_var,
	)
	var node_id interface{} = this.execCypher(cypher)
	switch {
	case node_id == nil:
		panic("Cannot create node: " + cypher)
	default:
		c <- int(node_id.(int64))
		fmt.Println("node_id:", node_id, time.Now().Unix())
		this.Done()
	}
}

func (this *j2n) getFieldsCypherPart(data interface{}, node_var, node_type string, c chan int) (cy []string) {
	switch node_type {
	case TYPE_OBJECT:
		for k, v := range data.(map[string]interface{}) {
			if f, ok := this.makeField(k, v, node_var, node_type, c); ok {
				cy = append(cy, f)
			}
		}
	case TYPE_ARRAY:
		for i, v := range data.([]interface{}) {
			if f, ok := this.makeField(i, v, node_var, node_type, c); ok {
				cy = append(cy, f)
			}
		}
	default:
		panic(fmt.Sprintf("Only '[]interface{}' and 'map[string]interface{}' are accepted in getFieldsCypherPart, given: '%T'", data))
	}

	return
}

func (this *j2n) makeField(k, v interface{}, parent_var, parent_type string, parent_c chan int) (f string, ok bool) {
	switch v.(type) {
	case string:
		v = strings.Replace(v.(string), "'", "\\'", -1)
	}
	var node_var string = fmt.Sprintf("%s_%v", parent_var, k)
	switch reflect.ValueOf(v).Kind() {
	case reflect.Array, reflect.Slice:
		//defer func(){go this.create_nested(k, parent_var, parent_type, TYPE_ARRAY, node_var, v)}()
		//defer this.create_nested(k, parent_var, parent_type, TYPE_ARRAY, node_var, v)
		this.Add(1)
		go this.create_nested(k, parent_var, parent_type, TYPE_ARRAY, node_var, v, parent_c)
	case reflect.Map:
		//defer func(){go this.create_nested(k, parent_var, parent_type, TYPE_OBJECT, node_var, v)}()
		//defer this.create_nested(k, parent_var, parent_type, TYPE_OBJECT, node_var, v)
		this.Add(1)
		go this.create_nested(k, parent_var, parent_type, TYPE_OBJECT, node_var, v, parent_c)
	case reflect.Struct:
		panic(fmt.Sprintf("Only '[]interface{}' and 'map[string]interface{}' are accepted in nested values, given: '%T'", v))
	default:
		if v == nil {
			v = ""
		}
		var key string
		switch k.(type) {
		case string:
			key = k.(string)
		case int:
			key = fmt.Sprintf("k_%v", k)
		}

		return fmt.Sprintf("%v:'%v'", key, v), true
	}

	return
}

func NewJ2N(conn golangNeo4jBoltDriver.Conn) J2N {
	return new(j2n).SetConn(conn)

}
