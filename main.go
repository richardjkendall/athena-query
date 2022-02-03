package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

const VERSION string = "0.0.1"

type SavedCfg struct {
	WorkGroup string `json:"workgroup"`
	Database  string `json:"database"`
}

var workGroup string
var database string

var outputMode string = "ascii"
var ddlEnabled bool = false
var showStats bool = false
var showHeader bool = true
var outputFile string = ""

func ProcessCommand(command string, cfg aws.Config, ctx context.Context) (bool, error) {
	bits := strings.Split(command, " ")
	switch bits[0] {
	case ".quit":
		fmt.Println("Goodbye.")
		os.Exit(0)
		return true, nil
	case ".exit":
		fmt.Println("Goodbye.")
		os.Exit(0)
		return true, nil
	case ".help":
		DisplayHelp()
		return true, nil
	case ".schema":
		_, err := GetSchema(database, workGroup, cfg, ctx)
		if err != nil {
			PrettyPrintAwsError(err)
		}
		return true, nil
	case ".output":
		if len(bits) != 2 {
			outputFile = ""
			return true, nil
		} else {
			outputFile = bits[1]
			return true, nil
		}
	case ".header":
		if len(bits) != 2 {
			return false, errors.New(".header expects an argument")
		} else {
			switch bits[1] {
			case "on":
				showHeader = true
				return true, nil
			case "off":
				showHeader = false
				return true, nil
			default:
				return false, fmt.Errorf(".header expects either 'on' or 'off', '%s' is unknown", bits[1])
			}
		}
	case ".ddl":
		if len(bits) != 2 {
			return false, errors.New(".ddl expects an argument")
		} else {
			switch bits[1] {
			case "on":
				ddlEnabled = true
				return true, nil
			case "off":
				ddlEnabled = false
				return true, nil
			default:
				return false, fmt.Errorf(".ddl expects either 'on' or 'off', '%s' is unknown", bits[1])
			}
		}
	case ".stats":
		if len(bits) != 2 {
			return false, errors.New(".stats expects an argument")
		} else {
			switch bits[1] {
			case "on":
				showStats = true
				return true, nil
			case "off":
				showStats = false
				return true, nil
			default:
				return false, fmt.Errorf(".stats expects either 'on' or 'off', '%s' is unknown", bits[1])
			}
		}
	case ".mode":
		if len(bits) != 2 {
			return false, errors.New(".mode expects an argument")
		} else {
			switch bits[1] {
			case "csv":
				outputMode = "csv"
				return true, nil
			case "ascii":
				outputMode = "ascii"
				return true, nil
			default:
				return false, fmt.Errorf(".mode expects either 'ascii' or 'csv', '%s' is unknown", bits[1])
			}
		}
	default:
		return false, errors.New("unknown command")
	}
}

func main() {
	// need to get the parameters
	workGroupParam := flag.String("work-group", "primary", "Work group the query should be executed in")
	databaseParam := flag.String("database", "", "Which database should be used for the query")
	flag.Parse()

	// print welcome
	fmt.Printf("AthenaQuery %s\n", VERSION)
	fmt.Println("Enter \".help\" for usage hints")

	// check if config file exists
	userHome, userHomeErr := os.UserHomeDir()
	if userHomeErr != nil {
		fmt.Println("Error: could not get home directory", userHomeErr)
		os.Exit(1)
	}
	mkdirError := os.MkdirAll(userHome+"/.athena-query", 0755)
	if mkdirError != nil {
		fmt.Println("Error: could not create .athena-query directory", mkdirError)
		os.Exit(1)
	}
	configFile, configFileError := os.Open(userHome + "/.athena-query/config.json")
	if configFileError != nil {
		// there is no config file (or we could not read it), so we should use the parameters that have been
		// provided, so lets check them
		if *workGroupParam == "" {
			fmt.Println("Error: 'work-group' should be specified")
			os.Exit(1)
		}
		workGroup = *workGroupParam
		if *databaseParam == "" {
			fmt.Println("Error: 'database' should be specified")
			os.Exit(1)
		}
		database = *databaseParam
	} else {
		byteVal, _ := ioutil.ReadAll(configFile)
		var cfg SavedCfg
		json.Unmarshal(byteVal, &cfg)
		// need to put the details into memory if they are available
		// TODO
	}

	// print AWS details
	fmt.Printf("Using workgroup %s and database %s\n", workGroup, database)

	// get AWS context
	ctx := context.TODO()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal("Error: could not get AWS credentials from default chain", err)
	}

	// print account details
	client := sts.NewFromConfig(cfg)
	identity, err := client.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		log.Fatal("Error: could not get AWS caller identity", err)
	}
	fmt.Printf("Account ID: %s, Identity Arn: %s\n", aws.ToString(identity.Account), aws.ToString(identity.Arn))

	// check workgroup
	workGroupOkay, checkWgErr := CheckWorkGroup(workGroup, cfg, ctx)
	if checkWgErr != nil {
		PrettyPrintAwsError(checkWgErr)
		os.Exit(1)
	}
	if !workGroupOkay {
		fmt.Printf("Error: this workgroup '%s' has no default output location specified\n", workGroup)
		os.Exit(1)
	}

	// into the main loop
	reader := bufio.NewReader(os.Stdin)
	var mode = 0 // 0 means this is a new line, 1 means an extension of a previous line
	var query = ""

	for {
		if mode == 0 {
			// new line
			fmt.Print("athenaquery> ")
		} else {
			// continuation of previous line
			fmt.Print("        ...> ")
		}
		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)

		// if empty just skip to next
		if text == "" {
			mode = 0
			query = ""
			continue
		}

		// check if this is a command
		if mode == 0 && strings.HasPrefix(text, ".") {
			// this is a command, so we need to process it
			_, commandErr := ProcessCommand(text, cfg, ctx)
			if commandErr != nil {
				fmt.Printf("Error: %s\n", commandErr)
			}
		} else {
			if strings.HasSuffix(text, ";") {
				// trim ; from text
				text = text[:len(text)-1]
				query = query + " " + text
				query = strings.Trim(query, " \t")
				mode = 0

				// check if this is ddl, if so we need to see if ddl is enabled, if not we don't run
				// TODO add any missing DDL prefixes
				if strings.HasPrefix(strings.ToUpper(query), "CREATE") ||
					strings.HasPrefix(strings.ToUpper(query), "ALTER") ||
					strings.HasPrefix(strings.ToUpper(query), "DROP") {

					if !ddlEnabled {
						fmt.Printf("Error: DDL not enabled\n")
						mode = 0
						query = ""
						continue
					}

				}

				// need to run query
				id, queryErr := StartQueryExec(query, workGroup, database, cfg, ctx)
				if queryErr != nil {
					// there's a problem
					// print the error and reset
					PrettyPrintAwsError(queryErr)
					mode = 0
					query = ""
				} else {
					fmt.Printf("Query id: %s\n", id)
					queryRes, getQueryErr := MonitorQuery(id, cfg, ctx)
					if getQueryErr != nil {
						PrettyPrintAwsError(getQueryErr)
					} else {
						if queryRes.Successful {
							if showStats {
								fmt.Printf("Stats: bytes scanned: %v, runtime: %v\n", *queryRes.Stats.DataScannedInBytes, *queryRes.Stats.EngineExecutionTimeInMillis)
							}
							//fmt.Printf("statment type = %s\n", queryRes.StmtType)
							// now we need to get the results
							rows, columns, getResultsErr := GetQueryResults(id, cfg, ctx)
							if getResultsErr != nil {
								PrettyPrintAwsError(getResultsErr)
							} else {
								OutputResults(rows, columns, outputMode == "csv", showHeader, outputFile, queryRes.StmtType)
							}
						}
					}
				}
				query = ""
			} else {
				mode = 1
				query = query + " " + text
			}
		}

	}

}
