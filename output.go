package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/jedib0t/go-pretty/v6/table"
)

func OutputResults(rows []types.Row, columns []types.ColumnInfo, csv bool, qryType string) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)

	var header table.Row
	header = append(header, "#")

	for _, col := range columns {
		header = append(header, *col.Name)
	}
	t.AppendHeader(header)

	start := 1
	if qryType == "UTILITY" {
		start = 0
	}

	for i, row := range rows[start:] {
		var data table.Row
		data = append(data, i)
		// for UTILITY type we need to split the first column by tabs first...
		if qryType == "UTILITY" {
			for _, col := range strings.Split(*row.Data[0].VarCharValue, "\t") {
				if col != "" {
					data = append(data, col)
				} else {
					data = append(data, "null")
				}
			}
		} else {
			for _, col := range row.Data {
				if col.VarCharValue != nil {
					data = append(data, *col.VarCharValue)
				} else {
					data = append(data, "null")
				}
			}
		}
		t.AppendRow(data)
	}
	if csv {
		t.RenderCSV()
	} else {
		t.Render()
	}

}

func DisplayHelp() {
	fmt.Println(".ddl\t\tEnable or disable DDL statements 'CREATE', 'ALTER' and 'DROP'")
	fmt.Println(".help\t\tDisplay this message")
	fmt.Println(".mode\t\tChange output mode")
	fmt.Println(".stats\t\tDisplay query stats")
	fmt.Println(".quit\t\tExit this utility")
}
