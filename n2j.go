package json2neo

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"strconv"
	"strings"
	"sync"
)

//TODO:: refactor to a better method!

/*
N2J is Neo4j to Json interface
*/
type N2J interface {
	SetStubNode(nodeID int64) N2J
	SetRootLabel(sl string) N2J
	SetRootName(n string) N2J
	SetConn(conn golangNeo4jBoltDriver.Conn) N2J
	SetRootNodeID(id int64) N2J
	WithID(b bool) N2J
	Retrieve() interface{}
}

type n2j struct {
	sync.Mutex
	sync.WaitGroup
	out              interface{}
	neoConn          golangNeo4jBoltDriver.Conn
	hasConn          bool
	rootID           int64
	rootType         string
	stubNodeID       int64
	stubNodeIDFilled bool
	stubNodeLabel    string
	stubNodeName     string
	multiRootFound   bool
	withID           bool
}

func (n2j *n2j) SetStubNode(nodeID int64) N2J {
	n2j.stubNodeID = nodeID
	n2j.stubNodeIDFilled = true
	n2j.findRootNodeIDByStub()
	return n2j
}

func (n2j *n2j) SetRootLabel(sl string) N2J {
	n2j.stubNodeLabel = sl
	n2j.findRootNodeIDByStub()
	return n2j
}

func (n2j *n2j) SetRootName(n string) N2J {
	n2j.stubNodeName = n
	n2j.findRootNodeIDByStub()
	return n2j
}

func (n2j *n2j) findRootNodeIDByStub() {
	var cypher = "MATCH %s(root%s) WHERE %s AND %v RETURN ID(root)"
	var label, name, id, preID string
	if n2j.stubNodeLabel != "" {
		label = ":" + strings.ToUpper(n2j.stubNodeLabel)
	}
	if n2j.stubNodeIDFilled {
		preID = fmt.Sprintf("(stub)-[rel%s]->", label)
		id = fmt.Sprintf("ID(stub) = %d", n2j.stubNodeID)
	} else {
		id = ValueTrue
	}
	if n2j.stubNodeName != "" {
		name = fmt.Sprintf("root.%s =~ '(?i)%s'", RootNameKey, n2j.stubNodeName)
	} else {
		name = ValueTrue
	}
	cypher = fmt.Sprintf(cypher,
		preID,
		label,
		id,
		name,
	)
	res, _, _, err := n2j.neoConn.QueryNeoAll(cypher, map[string]interface{}{})
	if err != nil {
		panic(err)
	}
	if len(res) == 0 {
		panic("stub_not_found")
	}
	n2j.rootID = res[0][0].(int64)
	n2j.multiRootFound = len(res) > 1
}

func (n2j *n2j) SetRootNodeID(id int64) N2J {
	n2j.rootID = id
	n2j.multiRootFound = false
	return n2j
}

func (n2j *n2j) SetConn(conn golangNeo4jBoltDriver.Conn) N2J {
	n2j.neoConn, n2j.hasConn = conn, true
	return n2j
}
func findTypeByLabels(labels []interface{}) string {
	if firstPlace(labels, LabelArrProp) >= 0 {
		return TypeToLabel[LabelArrProp]
	}
	if firstPlace(labels, LabelObjProp) >= 0 {
		return TypeToLabel[LabelObjProp]
	}
	return ""
}

func (n2j *n2j) WithID(b bool) N2J {
	n2j.withID = b
	return n2j
}

func (n2j *n2j) Retrieve() interface{} {
	if !n2j.hasConn {
		panic("neo4j_connection_not_found")
	}
	if n2j.multiRootFound {
		panic("multiple_root_nodes_found")
	}
	var cypher string
	n2j.queryBuilder(&cypher, n2j.maxLenFinder()+1)
	res, _, _, err := n2j.neoConn.QueryNeoAll(cypher, gin.H{})
	if err != nil {
		panic(err)
	}
	var result = res[0][0].(map[string]interface{})
	var rootLabels = result[LabelsKey].([]interface{})
	n2j.rootType = findTypeByLabels(rootLabels)
	delete(result, LabelsKey)
	n2j.out = n2j.makeNode(result, n2j.rootType)
	return n2j.out

}

func (n2j *n2j) makeNode(node map[string]interface{}, nodeType string) interface{} {
	//var nodeName string = node[ROOT_NAME_KEY]
	delete(node, RootNameKey)
	var outArray []interface{}
	var outObject map[string]interface{}
	if nodeType == "" {
		nodeType = node[TypeKey].(string)
	}
	delete(node, TypeKey)
	switch nodeType {
	case TypeObject:
		outObject = make(map[string]interface{})
	case TypeArray:
		if n2j.withID {
			outArray = make([]interface{}, getArrayNodeLen(node)-1)
		} else {
			outArray = make([]interface{}, getArrayNodeLen(node))
		}
	}
	if children, ok := node[DataKey]; ok {
		for _, child := range children.([]interface{}) {
			var childNode = child.(map[string]interface{})
			switch nodeType {
			case TypeObject:
				outObject[childNode[RootNameKey].(string)] = n2j.makeNode(childNode, "")
			case TypeArray:
				i, _ := strconv.Atoi(childNode[RootNameKey].(string))
				outArray[i] = n2j.makeNode(childNode, "")
			}
		}
		delete(node, DataKey)
	}
	for k, v := range node {
		switch nodeType {
		case TypeObject:
			outObject[k] = v
		case TypeArray:
			//TODO:: write a func for convert keys:
			if k == IDKey && n2j.withID {
				continue
			}
			i, _ := strconv.Atoi(strings.Split(k, "_")[1])
			outArray[i] = v
		}
	}
	switch nodeType {
	case TypeObject:
		return outObject
	case TypeArray:
		return outArray
	default:
		return nil
	}
}

func getArrayNodeLen(node map[string]interface{}) (cnt int) {
	if children, ok := node[DataKey]; ok {
		cnt += len(children.([]interface{})) - 1
	}
	return cnt + len(node)
}

func (n2j *n2j) maxLenFinder() int {
	var cypher = "match (n)-[a *..]->(leaf) where id(n) = {root_id} return a"
	res, _, _, err := n2j.neoConn.QueryNeoAll(cypher, gin.H{"root_id": n2j.rootID})
	if err != nil {
		panic(err)
	}
	var maxLen int
	for _, v := range res {
		var length = len(v[0].([]interface{}))
		if length > maxLen {
			maxLen = length
		}
	}
	return maxLen
}

func (n2j *n2j) queryBuilder(query *string, size int) {
	var idPart string
	*query += fmt.Sprintf("START root1=node(%d)\n", n2j.rootID)
	for i := 1; i < size; i++ {
		var iPlus = i + 1
		*query += fmt.Sprintf("OPTIONAL MATCH (root%d)-[rel%d]->(root%d)\n", i, iPlus, iPlus)
	}
	for index := size; index > 0; index-- {
		var data string
		if index != 1 {
			if n2j.withID {
				idPart = fmt.Sprintf(" ,%s:ID(root%d) ", IDKey, index)
			} else {
				idPart = ""
			}
			if index == size {
				data = fmt.Sprintf("WITH COLLECT(root%d {.* %s, %s:rel%d.type, %s:rel%d.name}) as root%d",
					index,
					idPart,
					TypeKey,
					index,
					RootNameKey,
					index,
					index,
				)

			} else {
				data = fmt.Sprintf("WITH COLLECT(root%d {.* %s, %s:rel%d.type, %s:rel%d.name, %s:root%d}) as root%d",
					index,
					idPart,
					TypeKey,
					index,
					RootNameKey,
					index,
					DataKey,
					index+1,
					index,
				)
			}
			for j := 1; j < index; j++ {
				if index == j {
					continue
				}
				data += fmt.Sprintf(",root%d", j)
				if j != 1 {
					data += fmt.Sprintf(",rel%d", j)
				}
			}
		} else {
			if n2j.withID {
				idPart = fmt.Sprintf(" ,%s:ID(root1) ", IDKey)
			} else {
				idPart = ""
			}
			data = fmt.Sprintf("WITH root1 {.* %s, %s:labels(root1), %s:root2} as root1",
				idPart,
				LabelsKey,
				DataKey,
			)
		}
		*query += fmt.Sprintf("%s\n", data)
	}
	*query += "RETURN root1"
}

/*
NewN2J is N2J factory method
*/
func NewN2J(conn golangNeo4jBoltDriver.Conn) N2J {
	return new(n2j).SetConn(conn)
}
