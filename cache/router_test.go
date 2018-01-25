package cache

import (
	. "gopkg.in/check.v1"
)

type RouterTestSuite struct{}

var _ = Suite(&RouterTestSuite{})

func (s *RouterTestSuite) TestRouter(c *C) {
	Build(c.MkDir())
	t := "bars"
	p := "NVDA_bats"

	// Add topic
	Add("bars")
	err := Update(t, p, AddPartition)
	c.Assert(err, IsNil)

	// Pull published topic addition
	add := <-PullAdditions()
	c.Assert(add.Topic, Equals, t)

	// Add partition
	err = Update(t, p, AddPartition)
	c.Assert(err, NotNil)

	// Pull published partition addition
	add = <-PullAdditions()
	c.Assert(add.Partition, Equals, p)

	Append(t, p, GenData())

	// Pull published ata
	pub := <-Pull()
	c.Assert(len(pub.(*Publication).Entries), Equals, 5)

	// Clear data
	Update(t, p, ClearPartition)

	// Remove partition
	Update(t, p, RemovePartition)
	rem := <-PullRemovals()
	c.Assert(rem.Partition, Equals, p)

	// Remove topic
	Remove(t)
	rem = <-PullRemovals()
	c.Assert(rem.Topic, Equals, t)
}
