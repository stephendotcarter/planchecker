package main

import (
	"fmt"
	"github.com/stephendotcarter/planchecker/plan"
	"os"
)

func main() {
	// Read filename from arguments
	filename := os.Args[1]

	// Create new explain object
	var explain plan.Explain

	// Init the explain from filename
	err := explain.InitFromFile(filename, true)
	if err != nil {
		fmt.Printf("%s\n", err)
		os.Exit(1)
	}

	// Print Plan
	explain.PrintPlan()
}
