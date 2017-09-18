package json2neo

import (
	"strings"
	"fmt"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

//TODO:: bejaye inhame param ye struct begiram
func DeleteBulkNodes(neo_conn golangNeo4jBoltDriver.Conn, stub_node_id int64, root_node_label, root_node_name string, except_node_id int64) (golangNeo4jBoltDriver.Result, error) {
	var cypher string = `MATCH %s(root%s) WHERE %s AND %v AND %s
			OPTIONAL MATCH (root)-[*..]->(leaf)
			DETACH DELETE root,leaf`
	var label, name, id, pre_id, except string
	if root_node_label != "" {
		label = ":" + strings.ToUpper(root_node_label)
	}
	if stub_node_id != -1 {
		pre_id = fmt.Sprintf("(stub)-[rel%s]->", label)
		id = fmt.Sprintf("ID(stub) = %d", stub_node_id)
	} else {
		id = VALUE_TRUE
	}
	if root_node_name != "" {
		name = fmt.Sprintf("root.%s = '%s'", ROOT_NAME_KEY, root_node_name)
	} else {
		name = VALUE_TRUE
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
	return neo_conn.ExecNeo(cypher, map[string]interface{}{})
}

func FindRootIDByFields(neo_conn golangNeo4jBoltDriver.Conn, root_node_label, root_node_name string, conditions map[string]interface{}) (int64, error) {
	var cypher string = `MATCH (root%s)-[*..]->(leaf)
			WHERE %s AND
			%s
			RETURN ID(root)`
	var conditions_cypher_stub string = "(root.%s =~ '%v' or leaf.%s =~ '%v') AND "
	var label, name, conditions_cypher string
	if root_node_label != "" {
		label = ":" + strings.ToUpper(root_node_label)
	}
	if root_node_name != "" {
		name = fmt.Sprintf("root.%s =~ '(?i)%s'", ROOT_NAME_KEY, root_node_name)
	} else {
		name = VALUE_TRUE
	}
	for key, value := range conditions {
		switch value.(type) {
		case string:
			value = "(?i)" + value.(string)
		}
		conditions_cypher += fmt.Sprintf(conditions_cypher_stub, key, value, key, value)
	}
	conditions_cypher += VALUE_TRUE
	cypher = fmt.Sprintf(cypher,
		label,
		name,
		conditions_cypher,
	)
	res, _, _, err := neo_conn.QueryNeoAll(cypher, conditions)
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
