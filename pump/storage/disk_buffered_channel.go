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

// TODO:
// 1. clear buf files on startup

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var writeBatchSize = 20000

const bufExt = "chanbuf"

type diskBufferedChannel struct {
	// C represents the memory part of the buffered channel
	C chan *request
	// Dir is the directory to be used for storage of buffer files
	Dir string
	// seq keeps track of the next sequence number for buffer files,
	// buffer files should be sorted acendingly with sequence number.
	seq         int
	memBuffer   []*request
	nPendingBuf int32

	bufNotEmpty *sync.Cond

	sync.Mutex
}

func newDiskBufferedChannel(c chan *request, dir string) *diskBufferedChannel {
	dbc := diskBufferedChannel{
		C:           c,
		Dir:         dir,
		bufNotEmpty: sync.NewCond(&sync.Mutex{}),
	}

	// TODO arrange quiting
	go dbc.consumeDiskBuf()
	return &dbc
}

// put is not threadsafe, because to keep the order of requests,
// we would only have one goroutine putting requests.
func (c *diskBufferedChannel) put(r *request) error {
	hasPending, err := c.hasPendingDiskBuf()
	if err != nil {
		return errors.Trace(err)
	}
	if !hasPending {
		select {
		case c.C <- r:
			return nil
		default:
		}
	}
	return c.write(r)
}

func (c *diskBufferedChannel) write(r *request) error {
	c.Lock()
	defer c.Unlock()
	c.memBuffer = append(c.memBuffer, r)
	// Count 1 pending buf if a new buf is started
	if len(c.memBuffer) == 1 {
		atomic.AddInt32(&c.nPendingBuf, 1)
		c.bufNotEmpty.Signal()
	}
	if len(c.memBuffer) >= writeBatchSize {
		err := c.writeToDiskBuf(c.memBuffer)
		if err != nil {
			return err
		}
		c.resetMemBuffer()
	}
	return nil
}

func (c *diskBufferedChannel) writeToDiskBuf(batch []*request) error {
	if len(batch) == 0 {
		return nil
	}
	tmpF, err := ioutil.TempFile(c.Dir, fmt.Sprintf("*.%s.tmp", bufExt))
	if err != nil {
		return errors.Trace(err)
	}
	defer os.Remove(tmpF.Name())
	if err = saveRequests(tmpF, batch); err != nil {
		return errors.Trace(err)
	}
	if err = tmpF.Chmod(0644); err != nil {
		return errors.Trace(err)
	}
	if err = tmpF.Sync(); err != nil {
		return errors.Trace(err)
	}
	if err = tmpF.Close(); err != nil {
		return errors.Trace(err)
	}
	basename := fmt.Sprintf("%07d.%s", c.seq, bufExt)
	path := filepath.Join(c.Dir, basename)
	// TODO check if new file already exists
	err = os.Rename(tmpF.Name(), path)
	if err != nil {
		return errors.Trace(err)
	}
	c.seq++
	return nil
}

func (c *diskBufferedChannel) consumeDiskBuf() {
	pattern := filepath.Join(c.Dir, "*."+bufExt)
	for {
		c.bufNotEmpty.L.Lock()
		for c.nPendingBuf <= 0 {
			c.bufNotEmpty.Wait()
		}
		c.bufNotEmpty.L.Unlock()

		// The only possible error returned is ErrBadPattern
		matches, _ := filepath.Glob(pattern)
		if len(matches) == 0 {
			if len(c.memBuffer) > 0 {
				c.Lock()
				reqs := make([]*request, len(c.memBuffer))
				copy(reqs, c.memBuffer)
				c.resetMemBuffer()
				c.Unlock()

				for _, r := range reqs {
					c.C <- r
				}
				atomic.AddInt32(&c.nPendingBuf, -1)
			}
			continue
		}
		sort.Slice(matches, func(i, j int) bool { return matches[i] < matches[j] })
		for _, path := range matches {
			log.Info("Reading buf file", zap.String("file", filepath.Base(path)))
			file, err := os.Open(path)
			if err != nil {
				// TODO: handle error
				return
			}
			bufReader := bufio.NewReader(file)
			reqs, err := readRequests(bufReader)
			if err != nil {
				// TODO: handle error
				return
			}
			for _, r := range reqs {
				c.C <- r
			}
			err = os.Remove(path)
			if err != nil {
				// TODO: handle error
				return
			}
			atomic.AddInt32(&c.nPendingBuf, -1)
		}
	}
}

func (c *diskBufferedChannel) resetMemBuffer() {
	for i := 0; i < len(c.memBuffer); i++ {
		c.memBuffer[i] = nil
	}
	c.memBuffer = c.memBuffer[:0]
}

func (c *diskBufferedChannel) hasPendingDiskBuf() (bool, error) {
	return atomic.LoadInt32(&c.nPendingBuf) > 0, nil
}

func saveRequests(f io.Writer, reqs []*request) error {
	var b bytes.Buffer
	for _, r := range reqs {
		data, err := r.MarshalBinary()
		if err != nil {
			return errors.Trace(err)
		}
		b.Write(data)
	}
	_, err := f.Write(b.Bytes())
	return errors.Trace(err)
}

func readRequests(f io.Reader) ([]*request, error) {
	reqs := make([]*request, 0, writeBatchSize)
	bytes := make([]byte, 24)
	for {
		_, err := f.Read(bytes)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		r := new(request)
		err = r.UnmarshalBinary(bytes)
		if err != nil {
			return nil, err
		}
		reqs = append(reqs, r)
	}
	return reqs, nil
}
