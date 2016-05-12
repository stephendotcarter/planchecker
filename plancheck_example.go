package main

import (
    "os"
    "./plan"
)

func main() {
    // Read filename from arguments
    filename := os.Args[1]

    // Create new explain object
    var explain plan.Explain

    // Init the explain from filename
    explain.InitFromFile(filename)

    // Print debug information
    explain.PrintDebug()
}
