package main

import (
	"fmt"
	"github.com/pivotal-gss/planchecker/plan"
	"io/ioutil"
	"os"
)

func main() {
	// Read filename from arguments
	filename := os.Args[1]

	// Check file exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		fmt.Printf("File does not exist")
		os.Exit(1)
	}

	// Read all lines
	filedata, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Printf("Could not read from file")
		os.Exit(1)
	}

	plantext := string(filedata)

	// Create new explain object
	var explain plan.Explain

	// Init the explain from filename
	err = explain.InitFromString(plantext, true)
	if err != nil {
		fmt.Printf("%s\n", err)
		os.Exit(1)
	}

	// Print Plan
	explain.PrintPlan()
}
