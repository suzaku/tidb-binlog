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

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	obinlog "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
)

func countMutations(tables []*obinlog.Table) map[string]map[string]int {
	count := make(map[string]map[string]int)
	for _, t := range tables {
		fullName := t.GetSchemaName() + "." + t.GetTableName()
		var (
			muts map[string]int
			ok   bool
		)
		if muts, ok = count[fullName]; !ok {
			count[fullName] = make(map[string]int)
			muts = count[fullName]
		}
		for _, m := range t.GetMutations() {
			tp := obinlog.MutationType_name[int32(*m.Type)]
			muts[tp] += 1
		}
	}
	return count
}

func describeBinlog(bl *obinlog.Binlog) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("Commit Ts: %d\n", bl.CommitTs))
	if bl.Type == obinlog.BinlogType_DDL {
		builder.WriteString("Type: DDL\n")
		builder.WriteString("Schema: " + bl.DdlData.GetSchemaName() + "\n")
		builder.WriteString("Table: " + bl.DdlData.GetTableName() + "\n")
		builder.WriteString("Query: " + string(bl.DdlData.GetDdlQuery()) + "\n")
	} else {
		builder.WriteString("Type: DML\n")
		mutations := countMutations(bl.DmlData.Tables)
		for tbl, count := range mutations {
			builder.WriteString(tbl + "\n")
			for tp, nMut := range count {
				builder.WriteString(fmt.Sprintf("\t%s:\t%d\n", tp, nMut))
			}
		}
	}
	return builder.String()
}

func dummyDMLBinlog() *obinlog.Binlog {
	var (
		schema       = "test"
		tbl1         = "tbl1"
		tbl2         = "tbl2"
		insert       = obinlog.MutationType(0)
		update       = obinlog.MutationType(1)
		val    int64 = 42
		row          = obinlog.Row{
			Columns: []*obinlog.Column{
				{Int64Value: &val},
			},
		}
	)
	return &obinlog.Binlog{
		Type:     obinlog.BinlogType_DML,
		CommitTs: 123,
		DmlData: &obinlog.DMLData{
			Tables: []*obinlog.Table{
				{
					SchemaName: &schema,
					TableName:  &tbl1,
					ColumnInfo: nil,
					Mutations: []*obinlog.TableMutation{
						{Type: &insert, Row: &row},
						{Type: &update, Row: &row},
						{Type: &insert, Row: &row},
					},
				},
				{
					SchemaName: &schema,
					TableName:  &tbl2,
					ColumnInfo: nil,
					Mutations: []*obinlog.TableMutation{
						{Type: &insert, Row: &row},
						{Type: &update, Row: &row},
						{Type: &update, Row: &row},
					},
				},
			},
		},
		DdlData: nil,
	}
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Please provide the file path of the message file.")
	}
	path := os.Args[1]
	data, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	//bl := dummyDMLBinlog()
	//data, err := bl.Marshal()
	//if err != nil {
	//	panic(err)
	//}
	binlog := &obinlog.Binlog{}
	err = binlog.Unmarshal(data)
	if err != nil {
		panic(err)
	}
	fmt.Println(describeBinlog(binlog))
}
