package main

import (
	"fmt"
	"github.com/stephendotcarter/planchecker/plan"
	"os"
)

func main() {

	// Create new explain object
	var explain plan.Explain

	// Init the explain from filename
	err := explain.InitFromStdin(true)
	if err != nil {
		fmt.Printf("%s\n", err)
		os.Exit(1)
	}

	// Print Plan
	explain.PrintPlan()
}
