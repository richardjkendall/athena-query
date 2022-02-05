package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

const VERSION string = "0.0.1"

type SavedCfg struct {
	WorkGroup string `json:"workgroup"`
	Database  string `json:"database"`
}

func ReadConfig() (SavedCfg, error) {
	var cfg SavedCfg
	cfg.Database = ""
	cfg.WorkGroup = ""
	userHome, userHomeErr := os.UserHomeDir()
	if userHomeErr != nil {
		fmt.Println("Error: could not get home directory", userHomeErr)
		return cfg, userHomeErr
	}
	mkdirError := os.MkdirAll(userHome+"/.athena-query", 0755)
	if mkdirError != nil {
		fmt.Println("Error: could not create .athena-query directory", mkdirError)
		return cfg, mkdirError
	}
	configFile, configFileError := os.Open(userHome + "/.athena-query/config.json")
	if configFileError != nil {
		// there is no config file (or we could not read it), so we should use the parameters that have been
		return cfg, nil
	}
	byteVal, _ := ioutil.ReadAll(configFile)
	json.Unmarshal(byteVal, &cfg)
	return cfg, nil
}

func WriteConfig(database string, workGroup string) error {
	userHome, userHomeErr := os.UserHomeDir()
	if userHomeErr != nil {
		fmt.Println("Error: could not get home directory", userHomeErr)
		return userHomeErr
	}
	mkdirError := os.MkdirAll(userHome+"/.athena-query", 0755)
	if mkdirError != nil {
		fmt.Println("Error: could not create .athena-query directory", mkdirError)
		return mkdirError
	}
	var cfg SavedCfg
	cfg.Database = database
	cfg.WorkGroup = workGroup
	data, marshalError := json.MarshalIndent(cfg, "", " ")
	if marshalError != nil {
		return marshalError
	}
	fileWriteError := ioutil.WriteFile(userHome+"/.athena-query/config.json", data, 0755)
	if fileWriteError != nil {
		return fileWriteError
	}
	return nil
}
