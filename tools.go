package json2neo

import (
	"fmt"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"strings"
)

//TODO:: bejaye inhame param ye struct begiram

/*
DeleteBulkNodes deletes J2N generated node-tree(s) using stub and root information
*/
func DeleteBulkNodes(neoConn golangNeo4jBoltDriver.Conn, stubNodeID int64, rootNodeLabel, rootNodeName string, exceptNodeID int64) (golangNeo4jBoltDriver.Result, error) {
	var cypher = `MATCH %s(root%s) WHERE %s AND %v AND %s
			OPTIONAL MATCH (root)-[*..]->(leaf)
			DETACH DELETE root,leaf`
	var label, name, id, preID, except string
	if rootNodeLabel != "" {
		label = ":" + strings.ToUpper(rootNodeLabel)
	}
	if stubNodeID != -1 {
		preID = fmt.Sprintf("(stub)-[rel%s]->", label)
		id = fmt.Sprintf("ID(stub) = %d", stubNodeID)
	} else {
		id = ValueTrue
	}
	if rootNodeName != "" {
		name = fmt.Sprintf("root.%s =~ '(?i)%s'", RootNameKey, rootNodeName)
	} else {
		name = ValueTrue
	}
	if exceptNodeID != -1 {
		except = fmt.Sprintf("ID(root) <> %v", exceptNodeID)
	}
	cypher = fmt.Sprintf(cypher,
		preID,
		label,
		id,
		name,
		except,
	)
	return neoConn.ExecNeo(cypher, map[string]interface{}{})
}

/*
FindRootIDByFields finds root nodeID using root node information
*/
func FindRootIDByFields(neoConn golangNeo4jBoltDriver.Conn, rootNodeLabel, rootNodeName string, conditions map[string]interface{}) (int64, error) {
	var cypher = `MATCH (root%s)-[*..]->(leaf)
			WHERE %s AND
			%s
			RETURN DISTINCT ID(root)`
	var conditionsCypherStub = "(root.%s =~ '%v' or leaf.%s =~ '%v') AND "
	var label, name, conditionsCypher string
	if rootNodeLabel != "" {
		label = ":" + strings.ToUpper(rootNodeLabel)
	}
	if rootNodeName != "" {
		name = fmt.Sprintf("root.%s =~ '(?i)%s'", RootNameKey, rootNodeName)
	} else {
		name = ValueTrue
	}
	for key, value := range conditions {
		switch value.(type) {
		case string:
			value = "(?i)" + value.(string)
		}
		conditionsCypher += fmt.Sprintf(conditionsCypherStub, key, value, key, value)
	}
	conditionsCypher += ValueTrue
	cypher = fmt.Sprintf(cypher,
		label,
		name,
		conditionsCypher,
	)
	res, _, _, err := neoConn.QueryNeoAll(cypher, conditions)
	if err != nil {
		panic(err)
	}
	if len(res) == 0 {
		panic("node_not_found")
	} else if len(res) > 1 {
		panic("multiple_root_nodes_found")
	} else {
		return res[0][0].(int64), err
	}
}

func firstPlace(s []interface{}, i interface{}) int {
	for index, value := range s {
		if value == i {
			return index
		}
	}
	return -1
}
