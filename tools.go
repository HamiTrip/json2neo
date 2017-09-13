package json2neo

import (
	"strings"
	"fmt"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

func DeleteBulkNodes(neo_conn golangNeo4jBoltDriver.Conn, stub_node_id int64, stub_node_label, stub_node_name string, except_node_id int64) (golangNeo4jBoltDriver.Result, error) {
	var cypher string = `MATCH %s(root%s) WHERE %s AND %v AND %s
			OPTIONAL MATCH (root)-[*..]->(leaf)
			DETACH DELETE root,leaf`
	var label, name, id , pre_id, except string
	if stub_node_label != "" {
		label = ":" + strings.ToUpper(stub_node_label)
	}
	if stub_node_id != -1 {
		pre_id=fmt.Sprintf("(stub)-[rel%s]->",label)
		id = fmt.Sprintf("ID(stub) = %d", stub_node_id)
	}else {
		id = "true"
	}
	if stub_node_name != "" {
		name = fmt.Sprintf("root.%s = '%s'", ROOT_NAME_KEY, stub_node_name)
	} else {
		name = "true"
	}
	if except_node_id != -1 {
		except = fmt.Sprintf("ID(root) <> %v", except_node_id)
	}
	cypher = fmt.Sprintf(cypher,
		pre_id,
		label,
		id,
		name,
		except,
	)
	res, err := neo_conn.ExecNeo(cypher, map[string]interface{}{})
	if err != nil {
		return res, err
	} else {
		return res, nil
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
