package json2neo

import (
	"github.com/gin-gonic/gin"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"sync"
	"strconv"
	"fmt"
	"strings"
	"hami/ums/base/log"
)

//TODO:: refactor to a better method!
type N2J interface {
	SetStubNode(node_id int64) N2J
	SetRootLabel(sl string) N2J
	SetRootName(n string) N2J
	SetConn(conn golangNeo4jBoltDriver.Conn) N2J
	SetRootNodeID(id int64) N2J
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
	switch len(root_labels) {
	case 2:
		this.root_type = TypeToLabel[root_labels[1].(string)]
	case 3:
		this.root_type = TypeToLabel[root_labels[2].(string)]
	}
	delete(result, LABELS_KEY)
	this.out = makeNode(result, this.root_type)
	return this.out

}

func makeNode(node map[string]interface{}, node_type string) interface{} {
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
		outArray = make([]interface{}, getArrayNodeLen(node))
	}
	if children, ok := node[DATA_KEY]; ok {
		for _, child := range children.([]interface{}) {
			var child_node map[string]interface{} = child.(map[string]interface{})
			switch node_type {
			case TYPE_OBJECT:
				outObject[child_node[ROOT_NAME_KEY].(string)] = makeNode(child_node, "")
			case TYPE_ARRAY:
				i, _ := strconv.Atoi(child_node[ROOT_NAME_KEY].(string))
				outArray[i] = makeNode(child_node, "")
			}
		}
		delete(node, DATA_KEY)
	}
	log.Warning("node:", node)
	for k, v := range node {
		switch node_type {
		case TYPE_OBJECT:
			outObject[k] = v
		case TYPE_ARRAY:
			//TODO:: write a func for convert keys:
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
	*query += fmt.Sprintf("START root1=node(%d)\n", this.root_id)
	for i := 1; i < size; i++ {
		itext := strconv.Itoa(i)
		iplus := strconv.Itoa(i + 1)
		*query += "OPTIONAL MATCH (root" + itext + ")-[rel" + iplus + "]->(root" + iplus + ")\n"
	}
	for i := size; i > 0; i-- {
		itext := strconv.Itoa(i)
		var data string
		if i != 1 {
			if i == size {
				data = "WITH COLLECT(root" + itext + " {.*, " + TYPE_KEY + ":rel" + itext + ".type, " + ROOT_NAME_KEY + ":rel" + itext + ".name}) as root" + itext
			} else {
				data = "WITH COLLECT(root" + itext + " {.*, " + TYPE_KEY + ":rel" + itext + ".type, " + ROOT_NAME_KEY + ":rel" + itext + ".name, " + DATA_KEY + ":root" + strconv.Itoa(i + 1) + "}) as root" + itext
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
			data = "WITH root1 {.*, " + LABELS_KEY + ":labels(root1), " + DATA_KEY + ":root2} as root1"
		}
		*query += data + "\n"
	}
	*query += "RETURN root1"
}

func NewN2J(conn golangNeo4jBoltDriver.Conn) N2J {
	return new(n2j).SetConn(conn)
}
