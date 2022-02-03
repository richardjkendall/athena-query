package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
)

type QuerySummary struct {
	Successful bool
	Stats      *types.QueryExecutionStatistics
	StmtType   string
}

func GetSchema(database string, workGroup string, cfg aws.Config, ctx context.Context) (string, error) {
	// get views
	getViewsSql := fmt.Sprintf("select table_name, view_definition from information_schema.views where table_schema='%s'", database)
	rows, _, err := RunQueryAndGetResults(getViewsSql, cfg, ctx)
	if err != nil {
		return "", err
	}
	views := map[string]int{}
	for _, view := range rows[1:] {
		fmt.Printf("CREATE VIEW %s AS\n", *view.Data[0].VarCharValue)
		sql := strings.TrimRight(*view.Data[1].VarCharValue, "\r\n")
		fmt.Printf("%s;\n\n", sql)
		views[*view.Data[0].VarCharValue] = 1
	}
	// get tables
	getTablesSql := fmt.Sprintf("select table_name from information_schema.tables where table_schema='%s'", database)
	rows, _, err = RunQueryAndGetResults(getTablesSql, cfg, ctx)
	if err != nil {
		return "", err
	}
	for _, table := range rows[1:] {
		if _, exists := views[*table.Data[0].VarCharValue]; !exists {
			getCreateTableSql := fmt.Sprintf("show create table %s", *table.Data[0].VarCharValue)
			lines, _, getCreateTableErr := RunQueryAndGetResults(getCreateTableSql, cfg, ctx)
			if getCreateTableErr != nil {
				return "", getCreateTableErr
			}
			for _, line := range lines {
				fmt.Println(*line.Data[0].VarCharValue)
			}
			fmt.Println("")
		}
	}
	return "", nil
}

func RunQueryAndGetResults(sql string, cfg aws.Config, ctx context.Context) ([]types.Row, []types.ColumnInfo, error) {
	queryId, queryErr := StartQueryExec(sql, workGroup, database, cfg, ctx)
	if queryErr != nil {
		return nil, nil, queryErr
	}
	queryRes, monitorErr := MonitorQuery(queryId, cfg, ctx)
	if monitorErr != nil {
		return nil, nil, monitorErr
	}
	if queryRes.Successful {
		rows, columns, getResultsErr := GetQueryResults(queryId, cfg, ctx)
		if getResultsErr != nil {
			return nil, nil, getResultsErr
		}
		return rows, columns, nil
	} else {
		return nil, nil, nil
	}
}

func CheckWorkGroup(workGroup string, cfg aws.Config, ctx context.Context) (bool, error) {
	wg, err := GetWorkGroup(workGroup, cfg, ctx)
	if err != nil {
		return false, err
	}
	if wg.WorkGroup.Configuration.ResultConfiguration.OutputLocation == nil {
		return false, nil
	} else {
		return true, nil
	}
}

func GetWorkGroup(workGroup string, cfg aws.Config, ctx context.Context) (athena.GetWorkGroupOutput, error) {
	client := athena.NewFromConfig(cfg)

	var gwgi athena.GetWorkGroupInput
	gwgi.WorkGroup = aws.String(workGroup)

	resp, err := client.GetWorkGroup(ctx, &gwgi)
	if err != nil {
		return athena.GetWorkGroupOutput{}, err
	}
	return *resp, nil

}

func StartQueryExec(query string, workgroup string, database string, cfg aws.Config, ctx context.Context) (string, error) {
	client := athena.NewFromConfig(cfg)

	var qei athena.StartQueryExecutionInput
	qei.WorkGroup = aws.String(workgroup)
	qei.QueryString = aws.String(query)

	var qec types.QueryExecutionContext
	qec.Catalog = aws.String("AwsDataCatalog")
	qec.Database = aws.String(database)

	qei.QueryExecutionContext = &qec

	queryExecution, err := client.StartQueryExecution(ctx, &qei)
	if err != nil {
		return "", err
	}

	return *queryExecution.QueryExecutionId, nil
}

func MonitorQuery(execId string, cfg aws.Config, ctx context.Context) (QuerySummary, error) {
	client := athena.NewFromConfig(cfg)

	check := true

	var gqei athena.GetQueryExecutionInput
	gqei.QueryExecutionId = aws.String(execId)

	var res QuerySummary

	for check {
		resp, err := client.GetQueryExecution(ctx, &gqei)
		if err != nil {
			res.Successful = false
			return res, err
		}
		state := resp.QueryExecution.Status.State
		stmtType := resp.QueryExecution.StatementType
		res.StmtType = string(stmtType)
		if state == "SUCCEEDED" {
			res.Stats = resp.QueryExecution.Statistics
			res.Successful = true
			return res, nil
		}
		if state == "FAILED" {
			res.Successful = false
			return res, errors.New(*resp.QueryExecution.Status.StateChangeReason)
		}
		if state == "CANCELLED" {
			res.Successful = false
			return res, errors.New("query was cancelled by user")
		}
		time.Sleep(2 * time.Second)
	}
	res.Successful = false
	return res, errors.New("could not get response")
}

func GetQueryResults(execId string, cfg aws.Config, ctx context.Context) ([]types.Row, []types.ColumnInfo, error) {
	client := athena.NewFromConfig(cfg)

	gqri := &athena.GetQueryResultsInput{
		QueryExecutionId: aws.String(execId),
	}

	var allRows []types.Row
	var columnInfo []types.ColumnInfo

	loop := true
	for loop {
		resp, err := client.GetQueryResults(ctx, gqri)
		if err != nil {
			return allRows, columnInfo, err
		}

		columnInfo = resp.ResultSet.ResultSetMetadata.ColumnInfo
		allRows = append(allRows, resp.ResultSet.Rows...)

		// check if we should keep getting results
		if resp.NextToken != nil {
			gqri.NextToken = resp.NextToken
		} else {
			loop = false
		}
	}

	return allRows, columnInfo, nil

}
