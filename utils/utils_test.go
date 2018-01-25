package utils

import (
	"io/ioutil"
	"testing"

	. "gopkg.in/check.v1"
)

type UtilsTestSuite struct{}

var _ = Suite(&UtilsTestSuite{})

func Test(t *testing.T) { TestingT(t) }

func (s *UtilsTestSuite) TestConfig(c *C) {
	data, err := ioutil.ReadFile("../slait.yaml")
	if err != nil {
		c.Fatalf("Failed to read default config file!")
	}
	err = ParseConfig(data)
	c.Assert(err, IsNil)
}
