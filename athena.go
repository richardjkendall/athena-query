package main

import (
	"context"
	"errors"
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
