package main

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/smithy-go"
)

func PrettyPrintAwsError(err error) {
	var oe *smithy.OperationError
	var ire *types.InvalidRequestException
	// is this an InvalidRequestException?
	if errors.As(err, &ire) {
		fmt.Printf("Error: %v\n", ire)
		return
	}
	if errors.As(err, &oe) {
		fmt.Printf("Error: %v\n", oe.Unwrap())
		return
	}
	// catch all
	fmt.Printf("Error: %s\n", err)
}
