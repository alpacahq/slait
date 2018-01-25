package commitlog

import (
	"fmt"
	"strings"
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type CommitLogTestSuite struct{}

var _ = Suite(&CommitLogTestSuite{})

func readEntries(path string, c *C) []*Entry {
	reader, err := NewReader(path)
	if err != nil {
		c.Fatal(err)
	}

	results := []*Entry{}
	for {
		entry, err := reader.Read()
		if entry == nil {
			break
		} else if err != nil {
			c.Fatal(err)
		}
		results = append(results, entry)
	}
	if err := reader.Close(); err != nil {
		c.Fatal(err)
	}

	return results
}

func (s *CommitLogTestSuite) TestAppend(c *C) {
	path := c.MkDir()

	clog, err := New(Options{
		Path:            path,
		MaxSegmentBytes: 16,
		CleanerOptions:  CleanerOptions{"MaxLogBytes": fmt.Sprintf("%d", 1024*1024)},
	})
	if err != nil {
		c.Fatal(err)
	}

	t1 := time.Date(2017, 8, 21, 16, 45, 13, 12, time.UTC)
	entry1 := &Entry{
		t1,
		[]byte(strings.Repeat("foobar", 10)),
	}
	if err := clog.Append(entry1); err != nil {
		c.Fatal(err)
	}

	pos1 := clog.Tell()

	t2 := time.Date(2017, 8, 21, 16, 45, 13, 95638, time.UTC)
	entry2 := &Entry{
		t2,
		[]byte(strings.Repeat("take2", 10)),
	}
	if err := clog.Append(entry2); err != nil {
		c.Fatal(err)
	}

	results1 := readEntries(path, c)
	c.Assert(len(results1), Equals, 2)
	c.Assert(results1[0].Timestamp, Equals, t1)
	c.Assert(string(results1[0].Data), Equals, strings.Repeat("foobar", 10))
	c.Assert(results1[1].Timestamp, Equals, t2)
	c.Assert(string(results1[1].Data), Equals, strings.Repeat("take2", 10))

	c.Assert(len(clog.segments), Equals, 2)

	clog.Truncate(pos1)

	results2 := readEntries(path, c)
	c.Assert(len(results2), Equals, 1)
	c.Assert(results1[0].Timestamp, Equals, t1)

	if err := clog.Close(); err != nil {
		c.Fatal(err)
	}

	if err := clog.DeleteAll(); err != nil {
		c.Fatal(err)
	}
}

func (s *CommitLogTestSuite) TestTrim(c *C) {
	path := c.MkDir()

	clog, err := New(Options{
		Path:            path,
		MaxSegmentBytes: 50,
		CleanerOptions: CleanerOptions{
			"Name":     "Duration",
			"Duration": "1h",
		},
	})
	if err != nil {
		c.Fatal(err)
	}

	t1 := time.Now().Add(-5 * time.Hour).UTC()
	for i := 0; i < 3; i++ {
		entry := &Entry{
			Timestamp: t1.Add(time.Duration(i) * time.Second),
			Data:      []byte(strings.Repeat("x", 10)),
		}
		if err := clog.Append(entry); err != nil {
			c.Fatal(err)
		}
		c.Assert(len(clog.segments), Equals, 1)
	}

	t2 := time.Now().Add(-30 * time.Minute).UTC()
	for i := 0; i < 3; i++ {
		entry := &Entry{
			Timestamp: t2.Add(time.Duration(i) * time.Second),
			Data:      []byte(strings.Repeat("z", 10)),
		}
		if err := clog.Append(entry); err != nil {
			c.Fatal(err)
		}
		c.Assert(len(clog.segments), Equals, 2)
	}

	upto, err := clog.Trim()
	if err != nil {
		c.Fatal(c)
	}

	c.Assert(len(clog.segments), Equals, 1)
	c.Assert(upto, Equals, t2)
}

func (s *CommitLogTestSuite) TestTrimLast(c *C) {
	path := c.MkDir()

	clog, err := New(Options{
		Path:            path,
		MaxSegmentBytes: 50,
		CleanerOptions: CleanerOptions{
			"Name":     "Duration",
			"Duration": "1h",
		},
	})
	if err != nil {
		c.Fatal(err)
	}
	t1 := time.Now().Add(-time.Hour).UTC()
	entry := &Entry{
		Timestamp: t1.Add(-time.Second),
		Data:      []byte(strings.Repeat("x", 10)),
	}
	if err := clog.Append(entry); err != nil {
		c.Fatal(err)
	}
	c.Assert(len(clog.segments), Equals, 1)

	upto, err := clog.Trim()
	if err != nil {
		c.Fatal(c)
	}

	c.Assert(len(clog.segments), Equals, 1)
	c.Assert(upto.IsZero(), Equals, true)
}
