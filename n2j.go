package json2neo

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/graph"
	"sync"
	"hami/ums/base"
)

type N2J interface {
	SetConn(conn golangNeo4jBoltDriver.Conn) N2J
	SetRootNodeID(id int64) N2J
	Retrieve() interface{}
}

type n2j struct {
	sync.Mutex
	sync.WaitGroup
	out        interface{}
	neo_conn   golangNeo4jBoltDriver.Conn
	has_conn   bool
	root_id    int64
	root_node  graph.Node
	root_name  string
	root_label string
	root_type  string
	nodes_map  map[int64]interface{}
}

func (this *n2j) SetRootNodeID(id int64) N2J {
	this.root_id = id
	return this
}
func (this *n2j) SetConn(conn golangNeo4jBoltDriver.Conn) N2J {
	this.neo_conn, this.has_conn = conn, true
	return this
}

func (this *n2j) Retrieve() interface{} {
	if !this.has_conn {
		panic("Neo4j connection not found!")
	}
	this.nodes_map = make(map[int64]interface{})
	//TODO:: modelaye dige ham bayad beshe peyda kone
	cypher := fmt.Sprintf("match (n:JN_ROOT_NODE)-[r *..]->(t) where id(n)=%d return n,r,t", this.root_id)
	res, _, _, err := this.neo_conn.QueryNeoAll(cypher, gin.H{})
	if err != nil {
		panic(err)
	}
	this.root_node = res[0][0].(graph.Node)
	switch len(this.root_node.Labels) {
	case 2:
		this.root_type = TypeToLabel[this.root_node.Labels[1]]
	case 3:
		this.root_label = this.root_node.Labels[0]
		this.root_type = TypeToLabel[this.root_node.Labels[2]]
	}
	this.root_name = this.root_node.Properties[ROOT_NAME_KEY].(string)
	delete(this.root_node.Properties, ROOT_NAME_KEY)
	this.root_id = this.root_node.NodeIdentity

	switch this.root_type {
	case TYPE_OBJECT:
		this.out = this.root_node.Properties
	case TYPE_ARRAY:
		this.out = extractArrayNode(this.root_node.Properties)
	}
	this.nodes_map[this.root_id] = this.out

	for _, v := range res {
		rel := v[1].([]interface{}) // len(rel) omgheshe ke age 1 bahse rahat
		node := v[2].(graph.Node).Properties

		lastRel := rel[len(rel) - 1].(graph.Relationship)

		this.addToOut(lastRel.StartNodeIdentity, lastRel.EndNodeIdentity, lastRel.Properties["name"], node, rel[0].(graph.Relationship).Properties["type"].(string))
	}

	return this.out
}

func (this *n2j)addToOut(parent_id, node_id int64, k, node interface{}, node_type string) {
	base.Warning("this.nodes_map:", this.nodes_map)
	base.Warning("parent_id:", parent_id)
	base.Warning("node_id:", node_id)

	switch this.nodes_map[parent_id].(type) {
	case map[string]interface{}:
		this.nodes_map[parent_id].(map[string]interface{})[k.(string)] = prepairNodeToAdd(node, node_type)

		this.nodes_map[node_id] = this.nodes_map[parent_id].(map[string]interface{})[k.(string)]

	case []interface{}:
		this.nodes_map[parent_id] = append(this.nodes_map[parent_id].([]interface{}), prepairNodeToAdd(node, node_type))

		this.nodes_map[node_id] = this.nodes_map[parent_id].([]interface{})[len(this.nodes_map[parent_id].([]interface{})) - 1]

	}
	base.Info("this.nodes_map[node_id]:",this.nodes_map[node_id])
	base.Info("this.out:",this.out)

}

func prepairNodeToAdd(v interface{}, node_type string) interface{} {
	switch node_type{
	case TYPE_ARRAY:
		return extractArrayNode(v.(map[string]interface{}))
	case TYPE_OBJECT:
		return v
	}
	return nil
}

func extractArrayNode(properties map[string]interface{}) (res []interface{}) {
	for _, v := range properties {
		res = append(res, v)
	}
	return
}

func NewN2J(conn golangNeo4jBoltDriver.Conn) N2J {
	return new(n2j).SetConn(conn)

}
