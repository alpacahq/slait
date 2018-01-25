package cache

import (
	"sync"
	"time"

	. "gopkg.in/check.v1"
)

func getFillTestData() Entries {
	t11 := time.Date(2017, 8, 21, 14, 15, 32, 95589, time.UTC)
	data11 := []byte(t11.Format(time.RFC3339))
	entry11 := &Entry{
		t11, data11,
	}

	t12 := time.Date(2017, 8, 21, 14, 17, 3, 3198, time.UTC)
	data12 := []byte(t12.Format(time.RFC3339))
	entry12 := &Entry{
		t12, data12,
	}

	t21 := time.Date(2017, 8, 21, 13, 35, 2, 589, time.UTC)
	data21 := []byte(t21.Format(time.RFC3339))
	entry21 := &Entry{
		t21, data21,
	}

	t22 := time.Date(2017, 8, 21, 15, 43, 19, 8313198, time.UTC)
	data22 := []byte(t12.Format(time.RFC3339))
	entry22 := &Entry{
		t22, data22,
	}

	return Entries{
		entry11, entry12, entry21, entry22,
	}
}

func (s *CacheTestSuite) TestFill(c *C) {
	dataDir := c.MkDir()

	cache1 := &Cache{
		topics:  &sync.Map{},
		dataDir: dataDir,
	}

	entries := getFillTestData()
	cache1.addTopic("topic1")
	cache1.addTopic("topic2")
	cache1.updateTopic("topic1", "part1", AddPartition)
	cache1.updateTopic("topic2", "abc", AddPartition)
	cache1.appendEntries("topic1", "part1", entries[0:2], true)
	cache1.appendEntries("topic2", "abc", entries[2:4], true)

	cache2 := Cache{
		topics:  &sync.Map{},
		dataDir: dataDir,
	}
	if err := cache2.fill(); err != nil {
		c.Fatal(err)
	}

	entries1 := cache2.get("topic1", "part1", nil, nil, 0)
	c.Assert(len(entries1), Equals, 2)
	c.Assert(entries1[0].Timestamp, Equals, entries[0].Timestamp)

	entries2 := cache2.get("topic2", "abc", nil, nil, 0)
	c.Assert(len(entries2), Equals, 2)
	c.Assert(string(entries2[1].Data), Equals, string(entries[3].Data))
}
