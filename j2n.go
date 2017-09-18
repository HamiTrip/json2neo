package json2neo

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"time"
)

/*
TODO::::: help haye LINT ro ejra konam
http://go-lint.appspot.com/github.com/HamiTrip/json2neo

TODO:: write tests and benchmarks
TODO:: support for struct!
TODO:: if wrote a method for get json instantly, must check json validity

TODO:: an update model that don't have to delete nodes and lost node IDs:
for example update each node with id! addition to normal update model
*/

/*
J2N is Json to Neo4j interface
 */
type J2N interface {
	SetStubNode(nodeID int64) J2N
	SetRootLabel(sl string) J2N
	SetRootName(n string) J2N
	SetConn(conn golangNeo4jBoltDriver.Conn) J2N
	Insert(data interface{}) (id int64, count int)
	Submit(data interface{}) (id int64, count int)

	execCypher(cypherPart string) interface{}
	cypherGenerator()
	createNested(nodeKey interface{}, parentVar, parentType, nodeType, nodeVar string, properties interface{}, parentChan chan int)
}

type j2n struct {
	sync.Mutex
	sync.WaitGroup
	totalNodes int
	neoConn    golangNeo4jBoltDriver.Conn
	hasConn    bool
	data       interface{}
	dataType   string
	rootID     int64
	rootName   string
	rootLabel  string
	hasStub    bool
	stubCypher string
}

func (j2n *j2n) SetStubNode(nodeID int64) J2N {
	j2n.stubCypher, j2n.hasStub = fmt.Sprintf("MATCH (%s) WHERE ID(%s) = %d\n", VarStub, VarStub, nodeID), true
	return j2n
}

func (j2n *j2n) SetConn(conn golangNeo4jBoltDriver.Conn) J2N {
	j2n.neoConn, j2n.hasConn = conn, true
	return j2n
}

func (j2n *j2n) SetRootLabel(rl string) J2N {
	if strings.Contains(rl, ":") {
		panic("Only one lable are accepted!")
	}
	j2n.rootLabel = fmt.Sprintf(":%s", strings.ToUpper(rl))
	return j2n
}

func (j2n *j2n) SetRootName(n string) J2N {
	j2n.rootName = n
	return j2n
}

func (j2n *j2n) Submit(data interface{}) (id int64, count int) {
	return j2n.Insert(data)
}

func (j2n *j2n) Insert(data interface{}) (id int64, count int) {
	if !j2n.hasConn {
		panic("Neo4j connection not found!")
	}
	j2n.data = data
	switch data.(type) {
	case []interface{}:
		j2n.dataType = TypeArray
	case map[string]interface{}:
		j2n.dataType = TypeObject
	default:
		panic(fmt.Sprintf("Only '[]interface{}' and 'map[string]interface{}' are accepted, given: '%T'", data))
	}
	j2n.cypherGenerator()
	j2n.Wait()
	return j2n.rootID, j2n.totalNodes
}

func (j2n *j2n) execCypher(cypherPart string) (res interface{}) {
	if strings.TrimSpace(cypherPart) != "" {
		j2n.Lock()
		result, err := j2n.neoConn.QueryNeo(cypherPart, map[string]interface{}{})
		if err == nil {
			r, _, _ := result.All()
			res = r[0][0]
			result.Close()
			j2n.totalNodes++
		} else {
			panic(err)
		}
		j2n.Unlock()
	}
	return
}

func (j2n *j2n) cypherGenerator() {
	//TODO:: Pipeline!
	var typeLabel string
	switch j2n.dataType {
	case TypeArray:
		typeLabel = LabelArrProp
	case TypeObject:
		typeLabel = LabelObjProp
	}
	var sfc string
	var c = make(chan int, 1)
	var fc = j2n.getFieldsCypherPart(j2n.data, VarRoot, j2n.dataType, c)
	if len(fc) > 0 {
		sfc = "," + strings.Join(fc, ",")
	}
	var cypher = fmt.Sprintf(
		"%s\n CREATE %s(%s%s:%s:%s {%s%s}) RETURN ID(%s)\n",
		j2n.stubCypher,
		j2n.getStubCypherPart(),
		VarRoot,
		j2n.rootLabel,
		typeLabel,
		LabelRootNode,
		j2n.getRootNameCypherPart(),
		sfc,
		VarRoot,
	)
	var nodeID = j2n.execCypher(cypher)
	switch {
	case nodeID == nil:
		panic("Cannot create root node!")
	default:
		c <- int(nodeID.(int64))
		j2n.rootID = nodeID.(int64)
		fmt.Println("root_node_id:", nodeID, time.Now().Unix())
	}

}

func (j2n *j2n) getStubCypherPart() (c string) {
	if j2n.hasStub {
		c = fmt.Sprintf("(%s)-[%s {%s:'%s'}]->", VarStub, j2n.rootLabel, RootNameKey, j2n.rootName)
	}
	return
}

func (j2n *j2n) getRootNameCypherPart() (c string) {
	if strings.TrimSpace(j2n.rootName) != "" {
		c = fmt.Sprintf("%s:'%s'",
			RootNameKey,
			j2n.rootName)
	}
	return
}

func (j2n *j2n) createNested(nodeKey interface{}, parentVar, parentType, nodeType, nodeVar string, properties interface{}, parentChan chan int) {
	var nodeTypeLabel string
	switch nodeType {
	case TypeArray:
		nodeTypeLabel = LabelArrProp
	case TypeObject:
		nodeTypeLabel = LabelObjProp
	}
	var c = make(chan int, 1)
	var sfc = strings.Join(j2n.getFieldsCypherPart(properties, nodeVar, nodeType, c), ",")
	var cName string
	if parentType == TypeObject {
		cName = fmt.Sprintf(",name:'%v'", nodeKey)
	} else if parentType == TypeArray {
		cName = fmt.Sprintf(",name:'%v'", nodeKey)
	}
	parentNodeID := <-parentChan
	parentChan <- parentNodeID
	var parentCypher = fmt.Sprintf("MATCH (%s) WHERE ID(%s) = %d\n", parentVar, parentVar, parentNodeID)
	var cypher = fmt.Sprintf(
		"%s \n CREATE (%s)-[:%s {type:'%s'%s}]->(%s:%s {%s}) RETURN ID(%s)\n",
		parentCypher,
		parentVar,
		LabelHasNested,
		nodeType,
		cName,
		nodeVar,
		nodeTypeLabel,
		sfc,
		nodeVar,
	)
	var nodeID = j2n.execCypher(cypher)
	switch {
	case nodeID == nil:
		panic("Cannot create node: " + cypher)
	default:
		c <- int(nodeID.(int64))
		j2n.Done()
	}
}

func (j2n *j2n) getFieldsCypherPart(data interface{}, nodeVar, nodeType string, nodeChan chan int) (cy []string) {
	switch nodeType {
	case TypeObject:
		for k, v := range data.(map[string]interface{}) {
			if f, ok := j2n.makeField(k, v, nodeVar, nodeType, nodeChan); ok {
				cy = append(cy, f)
			}
		}
	case TypeArray:
		for i, v := range data.([]interface{}) {
			if f, ok := j2n.makeField(i, v, nodeVar, nodeType, nodeChan); ok {
				cy = append(cy, f)
			}
		}
	default:
		panic(fmt.Sprintf("Only '[]interface{}' and 'map[string]interface{}' are accepted in getFieldsCypherPart, given: '%T'", data))
	}
	return
}

func (j2n *j2n) makeField(k, v interface{}, parentVar, parentType string, parentChan chan int) (f string, ok bool) {
	switch v.(type) {
	case string:
		v = strings.Replace(v.(string), "'", "\\'", -1)
	}
	var nodeVar = strings.Replace(fmt.Sprintf("%s_%v", parentVar, k), "-", "_", -1)
	switch reflect.ValueOf(v).Kind() {
	case reflect.Array, reflect.Slice:
		j2n.Add(1)
		go j2n.createNested(k, parentVar, parentType, TypeArray, nodeVar, v, parentChan)
	case reflect.Map:
		j2n.Add(1)
		go j2n.createNested(k, parentVar, parentType, TypeObject, nodeVar, v, parentChan)
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
			key = strings.Replace(key, "-", "_", -1)
		case int:
			key = fmt.Sprintf("k_%v", k)
		}
		return fmt.Sprintf("%v:'%v'", key, v), true
	}
	return
}

/*
NewJ2N is J2N factory method
 */
func NewJ2N(conn golangNeo4jBoltDriver.Conn) J2N {
	return new(j2n).SetConn(conn)

}
