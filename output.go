package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/jedib0t/go-pretty/v6/table"
)

func OutputResults(rows []types.Row, columns []types.ColumnInfo, csv bool) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)

	var header table.Row
	header = append(header, "#")

	for _, col := range columns {
		header = append(header, *col.Name)
	}
	t.AppendHeader(header)

	for i, row := range rows[1:] {
		var data table.Row
		data = append(data, i)
		for _, col := range row.Data {
			if col.VarCharValue != nil {
				data = append(data, *col.VarCharValue)
			} else {
				data = append(data, "null")
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
	fmt.Println(".help\t\tDisplay this message.")
	fmt.Println(".mode\t\tChange output mode")
	fmt.Println(".quit\t\tExit this utility")
}
