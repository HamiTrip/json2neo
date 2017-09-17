package json2neo

import (
	"github.com/gin-gonic/gin"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"sync"
	"strconv"
	"fmt"
	"strings"
)

//TODO:: refactor to a better method!
type N2J interface {
	SetStubNode(node_id int64) N2J
	SetRootLabel(sl string) N2J
	SetRootName(n string) N2J
	SetConn(conn golangNeo4jBoltDriver.Conn) N2J
	SetRootNodeID(id int64) N2J
	WithId(b bool) N2J
	Retrieve() interface{}
}

type n2j struct {
	sync.Mutex
	sync.WaitGroup
	out                 interface{}
	neo_conn            golangNeo4jBoltDriver.Conn
	has_conn            bool
	root_id             int64
	//root_node           graph.Node
	//root_name           string
	//root_label          string
	root_type           string
	stub_node_id        int64
	stub_node_id_filled bool
	stub_node_label     string
	stub_node_name      string
	multi_root_found    bool
	with_id             bool
}

func (this *n2j) SetStubNode(node_id int64) N2J {
	this.stub_node_id = node_id
	this.stub_node_id_filled = true
	this.findRootNodeIDByStub()
	return this
}

func (this *n2j) SetRootLabel(sl string) N2J {
	this.stub_node_label = sl
	this.findRootNodeIDByStub()
	return this
}

func (this *n2j) SetRootName(n string) N2J {
	this.stub_node_name = n
	this.findRootNodeIDByStub()
	return this
}

func (this *n2j) findRootNodeIDByStub() {
	var cypher string = "MATCH %s(root%s) WHERE %s AND %v RETURN ID(root)"
	var label, name, id, pre_id string
	if this.stub_node_label != "" {
		label = ":" + strings.ToUpper(this.stub_node_label)
	}
	if this.stub_node_id_filled {
		pre_id = fmt.Sprintf("(stub)-[rel%s]->", label)
		id = fmt.Sprintf("ID(stub) = %d", this.stub_node_id)
	} else {
		id = VALUE_TRUE
	}
	if this.stub_node_name != "" {
		name = fmt.Sprintf("root.%s = '%s'", ROOT_NAME_KEY, this.stub_node_name)
	} else {
		name = VALUE_TRUE
	}
	cypher = fmt.Sprintf(cypher,
		pre_id,
		label,
		id,
		name,
	)
	res, _, _, err := this.neo_conn.QueryNeoAll(cypher, map[string]interface{}{})
	if err != nil {
		panic(err)
	}
	if len(res) == 0 {
		panic("stub_not_found")
	}
	this.root_id = res[0][0].(int64)
	this.multi_root_found = len(res) > 1
}

func (this *n2j) SetRootNodeID(id int64) N2J {
	this.root_id = id
	this.multi_root_found = false
	return this
}

func (this *n2j) SetConn(conn golangNeo4jBoltDriver.Conn) N2J {
	this.neo_conn, this.has_conn = conn, true
	return this
}
func findTypeByLabels(labels []interface{}) string {
	if firstPlace(labels, L_ARR_PROP) >= 0 {
		return TypeToLabel[L_ARR_PROP]
	}
	if firstPlace(labels, L_OBJ_PROP) >= 0 {
		return TypeToLabel[L_OBJ_PROP]
	}
	return ""
}

func (this *n2j) WithId(b bool) N2J {
	this.with_id = b
	return this
}

func (this *n2j) Retrieve() interface{} {
	if !this.has_conn {
		panic("neo4j_connection_not_found")
	}
	if this.multi_root_found {
		panic("multiple_root_nodes_found")
	}
	var cypher string
	this.queryBuilder(&cypher, this.maxLenFinder() + 1)
	res, _, _, err := this.neo_conn.QueryNeoAll(cypher, gin.H{})
	if err != nil {
		panic(err)
	}
	var result map[string]interface{} = res[0][0].(map[string]interface{})
	var root_labels []interface{} = result[LABELS_KEY].([]interface{})
	this.root_type = findTypeByLabels(root_labels)
	delete(result, LABELS_KEY)
	this.out = this.makeNode(result, this.root_type)
	return this.out

}

func (this *n2j) makeNode(node map[string]interface{}, node_type string) interface{} {
	//var node_name string = node[ROOT_NAME_KEY]
	delete(node, ROOT_NAME_KEY)
	var outArray []interface{}
	var outObject map[string]interface{}
	if node_type == "" {
		node_type = node[TYPE_KEY].(string)
	}
	delete(node, TYPE_KEY)
	switch node_type {
	case TYPE_OBJECT:
		outObject = make(map[string]interface{})
	case TYPE_ARRAY:
		if this.with_id {
			outArray = make([]interface{}, getArrayNodeLen(node) - 1)
		} else {
			outArray = make([]interface{}, getArrayNodeLen(node))
		}
	}
	if children, ok := node[DATA_KEY]; ok {
		for _, child := range children.([]interface{}) {
			var child_node map[string]interface{} = child.(map[string]interface{})
			switch node_type {
			case TYPE_OBJECT:
				outObject[child_node[ROOT_NAME_KEY].(string)] = this.makeNode(child_node, "")
			case TYPE_ARRAY:
				i, _ := strconv.Atoi(child_node[ROOT_NAME_KEY].(string))
				outArray[i] = this.makeNode(child_node, "")
			}
		}
		delete(node, DATA_KEY)
	}
	for k, v := range node {
		switch node_type {
		case TYPE_OBJECT:
			outObject[k] = v
		case TYPE_ARRAY:
			//TODO:: write a func for convert keys:
			if k == ID_KEY && this.with_id {
				continue
			}
			i, _ := strconv.Atoi(strings.Split(k, "_")[1])
			outArray[i] = v
		}
	}
	switch node_type {
	case TYPE_OBJECT:
		return outObject
	case TYPE_ARRAY:
		return outArray
	default:
		return nil
	}
}

func getArrayNodeLen(node map[string]interface{}) (cnt int) {
	if children, ok := node[DATA_KEY]; ok {
		cnt += len(children.([]interface{})) - 1
	}
	return cnt + len(node)
}

func (this *n2j) maxLenFinder() int {
	var cypher string = "match (n)-[a *..]->(leaf) where id(n) = {root_id} return a"
	res, _, _, err := this.neo_conn.QueryNeoAll(cypher, gin.H{"root_id":this.root_id})
	if err != nil {
		panic(err)
	}
	var maxLen int = 0
	for _, v := range res {
		var len int = len(v[0].([]interface{}))
		if len > maxLen {
			maxLen = len
		}
	}
	return maxLen
}

func (this *n2j) queryBuilder(query *string, size int) {
	var id_part string
	*query += fmt.Sprintf("START root1=node(%d)\n", this.root_id)
	for i := 1; i < size; i++ {
		itext := strconv.Itoa(i)
		iplus := strconv.Itoa(i + 1)
		//*query += "OPTIONAL MATCH (root" + itext + ")-[rel" + iplus + "]->(root" + iplus + ")\n"
		*query += fmt.Sprintf("OPTIONAL MATCH (root%s)-[rel%s]->(root%s)\n", itext, iplus, iplus)
	}
	for i := size; i > 0; i-- {
		itext := strconv.Itoa(i)
		var data string
		if i != 1 {
			if this.with_id {
				id_part = fmt.Sprintf(" ,%s:ID(root%s) ", ID_KEY, itext)
			} else {
				id_part = ""
			}
			if i == size {
				//data = "WITH COLLECT(root" + itext + " {.*, " + TYPE_KEY + ":rel" + itext + ".type, " + ROOT_NAME_KEY + ":rel" + itext + ".name}) as root" + itext
				data = fmt.Sprintf("WITH COLLECT(root%s {.* %s, %s:rel%s.type, %s:rel%s.name}) as root%s",
					itext,
					id_part,
					TYPE_KEY,
					itext,
					ROOT_NAME_KEY,
					itext,
					itext,
				)

			} else {
				//data = "WITH COLLECT(root" + itext + " {.*, " + TYPE_KEY + ":rel" + itext + ".type, " + ROOT_NAME_KEY + ":rel" + itext + ".name, " + DATA_KEY + ":root" + strconv.Itoa(i + 1) + "}) as root" + itext
				data = fmt.Sprintf("WITH COLLECT(root%s {.* %s, %s:rel%s.type, %s:rel%s.name, %s:root%d}) as root%s",
					itext,
					id_part,
					TYPE_KEY,
					itext,
					ROOT_NAME_KEY,
					itext,
					DATA_KEY,
					i + 1,
					itext,
				)
			}

			for j := 1; j < i; j++ {
				if i == j {
					continue
				}
				jtext := strconv.Itoa(j)
				data += ",root" + jtext
				if j != 1 {
					data += ",rel" + jtext
				}
			}
		} else {
			if this.with_id {
				id_part = fmt.Sprintf(" ,%s:ID(root1) ", ID_KEY)
			} else {
				id_part = ""
			}
			data = "WITH root1 {.*, " + LABELS_KEY + ":labels(root1), " + DATA_KEY + ":root2} as root1"
			data = fmt.Sprintf("WITH root1 {.* %s, %s:labels(root1), %s:root2} as root1",
				id_part,
				LABELS_KEY,
				DATA_KEY,
			)
		}
		*query += data + "\n"
	}
	*query += "RETURN root1"
}

func NewN2J(conn golangNeo4jBoltDriver.Conn) N2J {
	return new(n2j).SetConn(conn)
}
