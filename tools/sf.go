package tools

import "github.com/bwmarrin/snowflake"

type Snowflake struct {
	nodeID int
	node   *snowflake.Node
}

func NewSnowflake(nodeID int64) *Snowflake {
	node, err := snowflake.NewNode(nodeID)
	if err != nil {
		panic(err)
	}
	return &Snowflake{node: node}
}

func (s *Snowflake) NextID() int64 {
	return s.node.Generate().Int64()
}
