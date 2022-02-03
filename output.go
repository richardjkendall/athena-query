package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/jedib0t/go-pretty/v6/table"
)

func OutputResults(rows []types.Row, columns []types.ColumnInfo, csv bool, header bool, outFile string, qryType string) error {
	t := table.NewWriter()
	//t.SetOutputMirror(os.Stdout)

	if header {
		var header table.Row
		header = append(header, "#")

		for _, col := range columns {
			header = append(header, *col.Name)
		}
		t.AppendHeader(header)
	}

	// we skip the first row (column names) unless it is a utility output
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
	var output = ""
	if csv {
		output = t.RenderCSV()
	} else {
		output = t.Render()
	}
	if outFile == "" {
		fmt.Println(output)
	} else {
		// we are writing to a file
		_, writeErr := WriteToFile(outFile, output)
		if writeErr != nil {
			return writeErr
		}
	}
	return nil
}

func WriteToFile(fileName string, output string) (bool, error) {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return false, err
	}
	file.WriteString(output + "\n")
	file.Close()
	return true, nil
}

func DisplayHelp() {
	fmt.Println(".ddl\t\tEnable or disable DDL statements 'CREATE', 'ALTER' and 'DROP'")
	fmt.Println(".exit\t\tSynonym for quit")
	fmt.Println(".file\t\tRun the commands in the file specified")
	fmt.Println(".header\t\tTurn on or off display of result set headers (column names)")
	fmt.Println(".help\t\tDisplay this message")
	fmt.Println(".mode\t\tChange output mode")
	fmt.Println(".output\t\tOutput to stdout or a file, if blank it uses stdout")
	fmt.Println(".save\t\tSave the default work-group and database for next time")
	fmt.Println(".schema\t\tPrint the schema of the database")
	fmt.Println(".stats\t\tDisplay query stats")
	fmt.Println(".quit\t\tExit this utility")
}
