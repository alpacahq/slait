package cache

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

type CacheTestSuite struct{}

var _ = Suite(&CacheTestSuite{})

func Test(t *testing.T) { TestingT(t) }

func (s *CacheTestSuite) TestCache(c *C) {
	masterDataDir := c.MkDir()
	Build(masterDataDir)

	// Create first topic + partition and add first batch of bars
	err := Add("bars")
	c.Assert(err, IsNil)
	err = Update("bars", "AMD_bats", AddPartition)
	c.Assert(err, IsNil)
	barData := GenData()
	err = Append("bars", "AMD_bats", barData)
	c.Assert(err, IsNil)
	barRet := Get("bars", "AMD_bats", nil, nil, 0)
	c.Assert(DataEqual(barRet, barData), Equals, true)
	end := barData[4].Timestamp.Add(-30 * time.Second)
	barRet = Get("bars", "AMD_bats", &time.Time{}, &end, 0)
	c.Assert(DataEqual(barRet, barData[0:4]), Equals, true)
	barRet = Get("bars", "AMD_bats", nil, nil, 1)
	c.Assert(DataEqual(barRet, barData[len(barData)-1:]), Equals, true)
	barRet = Get("bars", "AMD_bats", &barData[0].Timestamp, &barData[4].Timestamp, 0)
	c.Assert(DataEqual(barRet, barData), Equals, true)
	barRet = Get("bars", "AMD_bats", &barData[4].Timestamp, &barData[0].Timestamp, 0)
	c.Assert(barRet, IsNil)

	// Try to add an existing topic
	err = Add("bars")
	c.Assert(err, NotNil)

	// Get some non-existent topic
	c.Assert(Get("some unknown topic", "some partition", nil, nil, 0), IsNil)

	// Get from a non-existent partition
	c.Assert(Get("bars", "NVDA_bats", nil, nil, 0), IsNil)

	// Set to a non-existent topic
	err = Append("some unknown topic", "some partition", GenData())
	c.Assert(err, NotNil)

	// Set to a non-existent partition
	err = Append("bars", "AAPL_composite", GenData())
	c.Assert(err, IsNil)

	// no timestamp case
	Add("ztime")
	ztimeEntries := Entries{
		&Entry{Timestamp: time.Time{}, Data: []byte("abc")},
		&Entry{Timestamp: time.Time{}, Data: []byte("edf")},
	}
	err = Append("ztime", "NVDA", ztimeEntries)
	c.Assert(err, IsNil)
	c.Assert(ztimeEntries[0].Timestamp.IsZero(), Equals, false)

	err = Append("ztime", "NVDA", Entries{&Entry{Timestamp: time.Now().Add(-time.Second), Data: []byte("abc")}})
	c.Assert(err, NotNil)
	c.Assert(strings.HasPrefix(err.Error(), "Nothing new to append"), Equals, true)
	Remove("ztime")

	// Add a second topic + partition and a batch of quotes
	err = Add("quotes")
	c.Assert(err, IsNil)
	err = Update("quotes", "NVDA_composite", AddPartition)
	c.Assert(err, IsNil)
	quoteData := GenData()
	err = Append("quotes", "NVDA_composite", quoteData)
	c.Assert(err, IsNil)
	quoteRet := Get("quotes", "NVDA_composite", nil, nil, 0)
	c.Assert(DataEqual(quoteRet, quoteData), Equals, true)

	// Get all cached data
	allQuotes := GetAll("quotes", nil, nil, 0)
	c.Assert(len(allQuotes), Equals, 1)
	c.Assert(DataEqual(allQuotes["NVDA_composite"], quoteData), Equals, true)
	allQuotes = GetAll("quotes", nil, nil, 1)
	c.Assert(DataEqual(allQuotes["NVDA_composite"], quoteData[len(quoteData)-1:]), Equals, true)
	allBars := GetAll("bars", &barData[1].Timestamp, &barData[3].Timestamp, 0)
	c.Assert(len(allBars), Equals, 2)
	c.Assert(DataEqual(allBars["AMD_bats"], barData[1:4]), Equals, true)
	allFail := GetAll("some other type", nil, nil, 0)
	c.Assert(allFail, IsNil)
	allFail = GetAll("quotes", &quoteData[3].Timestamp, &quoteData[0].Timestamp, 0)
	c.Assert(len(allFail), Equals, 0)
	end = quoteData[4].Timestamp.Add(-30 * time.Second)
	allQuotes = GetAll("quotes", &time.Time{}, &end, 0)
	c.Assert(DataEqual(allQuotes["NVDA_composite"], quoteData[0:4]), Equals, true)

	// Remove a partition
	err = Update("bars", "AMD_bats", RemovePartition)
	c.Assert(err, IsNil)
	allBars = GetAll("bars", nil, nil, 0)
	c.Assert(len(allBars), Equals, 1)

	// Attempt to get data from the removed partition
	failData := Get("bars", "AMD_bats", nil, nil, 0)
	c.Assert(failData, IsNil)

	// Check that the catalog is valid
	cat := Catalog()
	c.Assert(len(cat), Equals, 2)
	c.Assert(len(cat["bars"]), Equals, 1)
	c.Assert(len(cat["quotes"]), Equals, 1)
	c.Assert(cat["quotes"]["NVDA_composite"], Equals, len(quoteData))

	// Remove a topic
	Remove("bars")
	cat = Catalog()
	c.Assert(len(cat), Equals, 1)

	// Update topic
	err = Update("bars", "INTC_composite", AddPartition)
	c.Assert(err, NotNil)
	err = Update("quotes", "NVDA_composite", AddPartition)
	c.Assert(err, NotNil)
	err = Update("quotes", "INTC_composite", RemovePartition)
	c.Assert(err, NotNil)
	err = Update("quotes", "INTC_composite", ClearPartition)
	c.Assert(err, NotNil)
	err = Update("quotes", "NVDA_composite", math.MaxInt16)
	c.Assert(err, NotNil)

	// Try to trim topic
	Trim()
	trimmedData := Get("quotes", "NVDA_composite", nil, nil, 0)
	c.Assert(len(trimmedData), Equals, len(quoteData))

	// Trim a non-existent topic
	masterCache.trimTopic("some random topic")
	trimmedData = Get("quotes", "NVDA_composite", nil, nil, 0)
	c.Assert(len(trimmedData), Equals, len(quoteData))

	// Get last commit
	lc := LastCommit()
	c.Assert(lc.Key, Equals, "quotes_NVDA_composite")
	c.Assert(lc.Timestamp, Equals, quoteData[len(quoteData)-1].Timestamp)

	// Get cache size
	size := Size()
	c.Assert(size, Equals, 0)

	// Clear a partition
	err = Update("quotes", "NVDA_composite", ClearPartition)
	c.Assert(err, IsNil)
	clearedData := Get("quotes", "NVDA_composite", nil, nil, 0)
	c.Assert(len(clearedData), Equals, 0)
}

func DataEqual(left, right Entries) bool {
	if len(left) != len(right) {
		return false
	}
	for i, e := range left {
		if e.Timestamp.Unix() != right[i].Timestamp.Unix() {
			return false
		}
		if !bytes.Equal(e.Data, right[i].Data) {
			return false
		}
	}
	return true
}

func (s *CacheTestSuite) TestTrim(c *C) {
	masterDataDir := c.MkDir()
	Build(masterDataDir)

	Add("topic1")
	Update("topic1", "key1", AddPartition)
	topic1, _ := masterCache.topics.Load("topic1")
	key1, _ := topic1.(*Topic).partitions.Load("key1")
	// hack to switch segments every record
	key1.(*Partition).clog.MaxSegmentBytes = 0
	var entries Entries
	t0 := time.Now().Add(-1*10*24*time.Hour + 12*time.Hour)
	for i := 0; i < 10; i++ {
		entries = append(entries, &Entry{
			Timestamp: t0.Add(time.Duration(i) * 24 * time.Hour),
			Data:      []byte("abc"),
		})
	}

	Append("topic1", "key1", entries)

	results1 := Get("topic1", "key1", nil, nil, 0)
	c.Assert(len(results1), Equals, 10)
	Trim()
	results2 := Get("topic1", "key1", nil, nil, 0)
	c.Assert(len(results2), Equals, 5)
}
