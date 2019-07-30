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
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"math"

	"github.com/pingcap/errors"
	obinlog "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
)

type Row struct {
	*obinlog.Table
}

func (r Row) HasPrimaryKey() bool {
	for _, col := range r.ColumnInfo {
		if col.IsPrimaryKey {
			return true
		}
	}
	return false
}

func (r Row) getMutation() *obinlog.TableMutation {
	return r.Mutations[0]
}

func (r Row) IsPrimaryKeyUpdated() (bool, error) {
	mut := r.getMutation()
	if mut.GetType() != obinlog.MutationType_Update {
		return false, nil
	}

	index := r.IndexPrimaryKeys()
	if len(index) == 0 {
		return false, nil
	}

	for _, i := range index {
		changed := mut.ChangeRow.Columns[i]
		new := mut.Row.Columns[i]
		equal, err := isPKValEqual(changed, new)
		if err != nil {
			return false, err
		}
		if !equal {
			return true, nil
		}
	}
	return false, nil
}

func (r Row) IndexPrimaryKeys() []int {
	var index []int
	for i, col := range r.ColumnInfo {
		if col.IsPrimaryKey {
			index = append(index, i)
		}
	}
	return index
}

func (r Row) Hash() (uint32, error) {
	// TODO: Check assumptions, eg. Only one row mutation; Has Primary Key
	keyBytes := []byte(r.GetSchemaName() + r.GetTableName())

	if r.HasPrimaryKey() {
		pkData, err := r.encodePKValues()
		if err != nil {
			return 0, err
		}
		keyBytes = append(keyBytes, pkData...)
	}

	h := fnv.New32()
	if _, err := h.Write(keyBytes); err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}

func (r *Row) encodePKValues() ([]byte, error) {
	var b bytes.Buffer
	cols := r.getMutation().Row.Columns
	pkIdx := r.IndexPrimaryKeys()
	for _, i := range pkIdx {
		colInfo := r.ColumnInfo[i]
		b.WriteString(colInfo.Name)
		col := cols[i]
		if col.BytesValue != nil {
			b.Write(col.GetBytesValue())
		} else if col.DoubleValue != nil {
			v := col.GetDoubleValue()
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v))
			b.Write(buf[:])
		} else if col.Int64Value != nil {
			v := col.GetInt64Value()
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], uint64(v))
			b.Write(buf[:])
		} else if col.StringValue != nil {
			b.WriteString(col.GetStringValue())
		} else if col.Uint64Value != nil {
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], col.GetUint64Value())
			b.Write(buf[:])
		} else {
			return nil, errors.Errorf("Invalid primary key `%s`: %+v", colInfo.Name, col)
		}
	}
	return b.Bytes(), nil
}

func isPKValEqual(c1 *obinlog.Column, c2 *obinlog.Column) (bool, error) {
	if c1.BytesValue != nil && c2.BytesValue != nil {
		return bytes.Equal(c1.GetBytesValue(), c2.GetBytesValue()), nil
	} else if c1.DoubleValue != nil && c2.DoubleValue != nil {
		return c1.GetDoubleValue() == c2.GetDoubleValue(), nil
	} else if c1.Int64Value != nil && c2.Int64Value != nil {
		return c1.GetInt64Value() == c2.GetInt64Value(), nil
	} else if c1.StringValue != nil && c2.StringValue != nil {
		return c1.GetStringValue() == c2.GetStringValue(), nil
	} else if c1.Uint64Value != nil && c2.Uint64Value != nil {
		return c1.GetUint64Value() == c2.GetUint64Value(), nil
	} else {
		return false, errors.Errorf("Uncomparable columns: %+v, %+v", c1, c2)
	}
}

func (r *Row) splitPKUpdate() (delete, insert *Row) {
	mut := r.getMutation()
	deleteMut := obinlog.TableMutation{
		Type: obinlog.MutationType_Delete.Enum(),
		Row:  mut.ChangeRow,
	}
	deleteRow := r.cloneWithNoMutation()
	deleteRow.Mutations = []*obinlog.TableMutation{&deleteMut}
	insertMut := obinlog.TableMutation{
		Type: obinlog.MutationType_Insert.Enum(),
		Row:  mut.Row,
	}
	insertRow := r.cloneWithNoMutation()
	insertRow.Mutations = []*obinlog.TableMutation{&insertMut}

	return deleteRow, insertRow
}

func (r *Row) cloneWithNoMutation() *Row {
	tbl := obinlog.Table{
		SchemaName: r.SchemaName,
		TableName:  r.TableName,
		ColumnInfo: r.ColumnInfo,
	}
	return &Row{&tbl}
}
