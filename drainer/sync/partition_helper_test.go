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

package sync

import (
	"github.com/gogo/protobuf/proto"
	. "github.com/pingcap/check"
	binlog "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
)

type rowSuite struct{}

var _ = Suite(&rowSuite{})

func (rs *rowSuite) TestHasPrimaryKey(c *C) {
	tbl := binlog.Table{
		ColumnInfo: []*binlog.ColumnInfo{
			{IsPrimaryKey: false},
			{IsPrimaryKey: false},
			{IsPrimaryKey: false},
		},
	}
	row := Row{&tbl}
	c.Assert(row.HasPrimaryKey(), IsFalse)

	tbl.ColumnInfo[1].IsPrimaryKey = true
	c.Assert(row.HasPrimaryKey(), IsTrue)
}

func (rs *rowSuite) TestIndexPrimaryKeys(c *C) {
	tbl := binlog.Table{
		ColumnInfo: []*binlog.ColumnInfo{
			{IsPrimaryKey: true},
			{IsPrimaryKey: false},
			{IsPrimaryKey: true},
			{IsPrimaryKey: false},
		},
	}
	row := Row{&tbl}
	c.Assert(row.IndexPrimaryKeys(), DeepEquals, []int{0, 2})
}

func (rs *rowSuite) TestIsPrimaryKeyUpdated(c *C) {
	tbl := binlog.Table{
		ColumnInfo: []*binlog.ColumnInfo{
			{Name: "gid", IsPrimaryKey: true},
			{IsPrimaryKey: false},
		},
		Mutations: []*binlog.TableMutation{
			{
				Type: binlog.MutationType_Insert.Enum(),
				Row:  &binlog.Row{},
			},
		},
	}
	row := Row{&tbl}
	updated, err := row.IsPrimaryKeyUpdated()
	c.Assert(err, IsNil)
	c.Assert(updated, IsFalse, Commentf("Not a update mutation"))

	mut := tbl.Mutations[0]
	mut.Type = binlog.MutationType_Update.Enum()
	mut.ChangeRow = &binlog.Row{
		Columns: []*binlog.Column{
			{Int64Value: proto.Int64(123)},
			{},
		},
	}
	mut.Row = &binlog.Row{
		Columns: []*binlog.Column{
			{Int64Value: proto.Int64(423423)},
			{},
		},
	}
	updated, err = row.IsPrimaryKeyUpdated()
	c.Assert(err, IsNil)
	c.Assert(updated, IsTrue, Commentf("ID changed"))
}

func (rs *rowSuite) TestIsPKValEqual(c *C) {
	eq, err := isPKValEqual(
		&binlog.Column{Int64Value: proto.Int64(4)},
		&binlog.Column{Int64Value: proto.Int64(4)},
	)
	c.Assert(err, IsNil)
	c.Assert(eq, IsTrue)
	eq, err = isPKValEqual(
		&binlog.Column{Int64Value: proto.Int64(4)},
		&binlog.Column{Int64Value: proto.Int64(10)},
	)
	c.Assert(err, IsNil)
	c.Assert(eq, IsFalse)

	eq, err = isPKValEqual(
		&binlog.Column{Uint64Value: proto.Uint64(10)},
		&binlog.Column{Uint64Value: proto.Uint64(12)},
	)
	c.Assert(err, IsNil)
	c.Assert(eq, IsFalse)

	eq, err = isPKValEqual(
		&binlog.Column{BytesValue: []byte("test")},
		&binlog.Column{BytesValue: []byte("test")},
	)
	c.Assert(err, IsNil)
	c.Assert(eq, IsTrue)

	_, err = isPKValEqual(
		&binlog.Column{BytesValue: []byte("sdf")},
		&binlog.Column{Uint64Value: proto.Uint64(33)},
	)
	c.Assert(err, ErrorMatches, ".*Uncomparable columns.*")
}

func (rs *rowSuite) TestHash(c *C) {
	tblName := "test_user"
	tbl := binlog.Table{
		TableName: &tblName,
		ColumnInfo: []*binlog.ColumnInfo{
			{Name: "gid", IsPrimaryKey: true},
			{IsPrimaryKey: false},
			{Name: "name", IsPrimaryKey: true},
		},
		Mutations: []*binlog.TableMutation{
			{
				Row: &binlog.Row{},
			},
		},
	}
	row := Row{&tbl}

	var hashes []uint32
	fakeIDs := []int64{1983, 1984, 1985, 1986, 1987}
	for _, id := range fakeIDs {
		tbl.Mutations[0].Row.Columns = []*binlog.Column{
			{Int64Value: &id},
			{},
			{StringValue: proto.String("Joestar")},
		}
		h, err := row.Hash()
		c.Assert(err, IsNil)
		hashes = append(hashes, h)
	}

	set := make(map[uint32]struct{})
	for _, h := range hashes {
		set[h] = struct{}{}
	}
	c.Assert(len(set), Equals, len(hashes))
}

func (rs *rowSuite) TestSplitPKUpdate(c *C) {
	tblName := "test_user"
	tbl := binlog.Table{
		TableName: &tblName,
		ColumnInfo: []*binlog.ColumnInfo{
			{Name: "id", IsPrimaryKey: true},
			{Name: "name"},
		},
		Mutations: []*binlog.TableMutation{
			{
				Type:      binlog.MutationType_Update.Enum(),
				Row:       &binlog.Row{},
				ChangeRow: &binlog.Row{},
			},
		},
	}
	row := Row{&tbl}

	rDelete, rInsert := row.splitPKUpdate()
	c.Assert(rDelete.Mutations[0].GetType(), Equals, binlog.MutationType_Delete)
	c.Assert(rInsert.Mutations[0].GetType(), Equals, binlog.MutationType_Insert)
}
