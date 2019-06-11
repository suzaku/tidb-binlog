// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"path/filepath"
	"time"

	. "github.com/pingcap/check"
	pb "github.com/pingcap/tipb/go-binlog"
)

type diskBufferedChanSuite struct{}

var _ = Suite(&diskBufferedChanSuite{})

func (s *diskBufferedChanSuite) TestUseChanDirectly(c *C) {
	d := c.MkDir()
	ch := make(chan *request, 10)
	dbc := newDiskBufferedChannel(ch, d)
	for i := 0; i < 5; i++ {
		err := dbc.put(&request{commitTS: int64(i)})
		c.Assert(err, IsNil)
	}
	c.Assert(len(dbc.C), Equals, 5)
}

func (s *diskBufferedChanSuite) TestCanPutMoreItemsThanChannelCap(c *C) {
	d := c.MkDir()
	ch := make(chan *request, 3)
	dbc := newDiskBufferedChannel(ch, d)
	nReqs := cap(ch) * 10
	for i := 0; i < nReqs; i++ {
		err := dbc.put(&request{commitTS: int64(i)})
		c.Assert(err, IsNil)
	}
	c.Assert(len(dbc.C), Equals, 3)

	var reqs []*request
	for i := 0; i < nReqs; i++ {
		select {
		case r := <-dbc.C:
			reqs = append(reqs, r)
		case <-time.After(500 * time.Millisecond):
			c.Fatalf("Timeout waiting for the %dth buffered request", i+1)
		}
	}
}

func (s *diskBufferedChanSuite) TestReadFromDiskBuffers(c *C) {
	orig := writeBatchSize
	writeBatchSize = 10
	defer func() {
		writeBatchSize = orig
	}()

	d := c.MkDir()
	ch := make(chan *request, 2)
	dbc := newDiskBufferedChannel(ch, d)
	nReqs := 100
	for i := 0; i < nReqs; i++ {
		err := dbc.put(&request{
			tp:       pb.BinlogType_Commit,
			commitTS: int64(i),
		})
		c.Assert(err, IsNil)
	}
	bufPaths, err := filepath.Glob(filepath.Join(d, "*."+bufExt))
	c.Assert(err, IsNil)
	// The requests distribution should be like this:
	// In the channel: 2
	// In the disk buffer files: 9 * 10
	// In the mem buffer: 8
	c.Assert(len(ch), Equals, 2)
	c.Assert(bufPaths, HasLen, 9)
	c.Assert(dbc.memBuffer, HasLen, 8)

	for i := 0; i < 90; i++ {
		select {
		case r := <-ch:
			c.Assert(r.commitTS, Equals, int64(i))
		case <-time.After(500 * time.Millisecond):
			c.Fatalf("Timeout waiting for the %dth buffered request", i+1)
		}
	}

	bufPaths, err = filepath.Glob(filepath.Join(d, "*."+bufExt))
	c.Assert(err, IsNil)
	c.Assert(bufPaths, HasLen, 1)

	for i := 90; i < nReqs; i++ {
		select {
		case r := <-ch:
			c.Assert(r.commitTS, Equals, int64(i))
		case <-time.After(500 * time.Millisecond):
			c.Fatalf("Timeout waiting for the %dth buffered request", i+1)
		}
	}
}
