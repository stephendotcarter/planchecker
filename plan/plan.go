package plan

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"
	"errors"
)

type Node struct {
	Operator string
	Indent   int
	Slice    int64
	Cost     string
	Rows     int64
	Width    int64
	RowStat RowStat
	RawLines []string
}

type RowStat struct {
	InOut string
	Rows float64
	Avg float64
	Max float64
	First float64
	End float64
	Offset float64
	Workers int64
}

type SliceStat struct {
	Name string
	MemoryAvg int64
	Workers int64
	MemoryMax int64
	WorkMem int64
	WorkMemWanted int64
}

type StatmentStat struct {
	MemoryUsed int64
	MemoryWanted int64
}

type Setting struct {
	Name  string
	Value string
}

type Explain struct {
	Nodes          []*Node
	SliceStats     []string
	StatementStats StatmentStat
	Settings       []Setting
	Optimizer      string
	Runtime        float64

	lines        []string
	lineOffset   int
	planFinished bool
}

var (
	patterns = map[string]*regexp.Regexp{
		"NODE":               regexp.MustCompile(`(.*) \(cost=(.*) rows=(.*) width=(.*)\)`),
		"SLICE":              regexp.MustCompile(`(.*)  \(slice(.*); segments: (.*)\)`),
		"SUBPLAN":            regexp.MustCompile(` SubPlan `),
		
		"ROWSTAT":            regexp.MustCompile(`Rows (out|in): `),
		"ROWSTAT_ROWS":       regexp.MustCompile(`Rows (out|in):  ([0-9.-]{1,}) rows`),
		"ROWSTAT_AVG":        regexp.MustCompile(`Avg ([0-9.-]{1,}) rows x ([0-9.-]{1,}) workers.*  Max ([0-9.-]{1,}) rows`),
		"ROWSTAT_FIRST":      regexp.MustCompile(`with ([0-9.-]{1,}) ms to first`),
		"ROWSTAT_END_START":  regexp.MustCompile(` ([0-9.-]{1,}) ms to end, start offset by (.*) ms.`),

		"SLICESTATS":           regexp.MustCompile(` Slice statistics:`),
		"SLICESTATS_1":         regexp.MustCompile(`\((slice[0-9]{1,})\).*Executor memory: ([0-9]{1,})K bytes`),
		"SLICESTATS_2":         regexp.MustCompile(`avg x ([0-9]+) workers, ([0-9]+)K bytes max \((seg[0-9]+)\)\.`),
		"SLICESTATS_3":         regexp.MustCompile(`Work_mem: ([0-9]+)K bytes max.`),
		"SLICESTATS_4":         regexp.MustCompile(`([0-9]+)K bytes wanted.`),

		"STATEMENTSTATS":         regexp.MustCompile(` Statement statistics:`),
		"STATEMENTSTATS_USED":    regexp.MustCompile(`Memory used: ([0-9.-]{1,})K bytes`),
		"STATEMENTSTATS_WANTED":  regexp.MustCompile(`Memory wanted: ([0-9.-]{1,})K bytes`),

		"SETTINGS":           regexp.MustCompile(` Settings: `),
		"OPTIMIZER":          regexp.MustCompile(` Optimizer status: `),
		"RUNTIME":            regexp.MustCompile(` Total runtime: `),
	}

	//currentNode *Node
)


// Reading files requires checking most calls for errors.
// This helper will streamline our error checks below.
func check(e error) {
	if e != nil {
		panic(e)
	}
}


func getIndent(line string) int {
	// Diff on line minus left whitspace (minus 3 as everything is indented)
	return len(line) - len(strings.TrimLeft(line, " "))
}


//func parseNode(line string) {
//    fmt.Println("NODE")
//
//    groups := patterns["node"].FindStringSubmatch(line)
//    operator := strings.TrimSpace(groups[1])
//    cost := strings.TrimSpace(groups[2])
//    rows := strings.TrimSpace(groups[3])
//    width := strings.TrimSpace(groups[4])
//    fmt.Printf("\toperator: %s\n\tcost: %s\n\trows: %s\n\twidth: %s\n", operator, cost, rows, width)
//}


func parseNode(line string) *Node {
	// Set node indent
	// Rest of node parsing is handled in parseNodeRawLines
	node := new(Node)
	node.Indent = getIndent(line)
	node.RawLines = []string{
		line,
	}

	return node
}


func parseRowStat(line string) RowStat {
	var ro RowStat

	ro.InOut = ""
	ro.Rows = -1
	ro.Avg = -1
	ro.Max = -1
	ro.First = -1
	ro.End = -1
	ro.Offset = -1
	ro.Workers = -1

	line = strings.TrimSpace(line)
	fmt.Println("\n", line)

	groups := patterns["ROWSTAT"].FindStringSubmatch(line)
	if len(groups) == 2 {
		ro.InOut = strings.TrimSpace(groups[1])
	}

	groups = patterns["ROWSTAT_ROWS"].FindStringSubmatch(line)
	if len(groups) == 2 {
		fmt.Println("ROWS", groups)
		ro.InOut = strings.TrimSpace(groups[1])
		ro.Rows, _ = strconv.ParseFloat(strings.TrimSpace(groups[2]), 64)
	}

	groups = patterns["ROWSTAT_AVG"].FindStringSubmatch(line)
	if len(groups) == 4 {
	fmt.Println("AVG", groups)
		ro.Avg, _ = strconv.ParseFloat(strings.TrimSpace(groups[1]), 64)
		ro.Workers, _ = strconv.ParseInt(strings.TrimSpace(groups[2]), 10, 64)
		ro.Max, _ = strconv.ParseFloat(strings.TrimSpace(groups[3]), 64)
	}

	groups = patterns["ROWSTAT_FIRST"].FindStringSubmatch(line)
	if len(groups) == 2 {
	fmt.Println("FIRST", groups)
		ro.First, _ = strconv.ParseFloat(strings.TrimSpace(groups[1]), 64)
	}

	groups = patterns["ROWSTAT_END_START"].FindStringSubmatch(line)
	if len(groups) == 3 {
	fmt.Println("END_START", groups)
		ro.End, _ = strconv.ParseFloat(strings.TrimSpace(groups[1]), 64)
		ro.Offset, _ = strconv.ParseFloat(strings.TrimSpace(groups[2]), 64)
	}

	return ro
}


func parseNodeRawLines(n *Node) error {
	// line 0 will always be the node line
	// Example: ->  Broadcast Motion 1:2  (slice1)  (cost=0.00..27.48 rows=1124 width=208)
	line := n.RawLines[0]

	groups := patterns["NODE"].FindStringSubmatch(line)
	fmt.Println("GROUPS:", groups)

	if len(groups) == 5 {
		// Remove the indent arrow
		groups[1] = strings.Trim(groups[1], " ->")

		// Check if the string contains slice information
		sliceGroups := patterns["SLICE"].FindStringSubmatch(groups[1])
		if len(sliceGroups) == 4 {
			n.Operator = strings.TrimSpace(sliceGroups[1])
			n.Slice, _ = strconv.ParseInt(strings.TrimSpace(sliceGroups[2]), 10, 64)
		// Else it's just the operator
		} else {
			n.Operator = strings.TrimSpace(groups[1])
			n.Slice = -1
		}

		// Store the remaining params
		n.Cost = strings.TrimSpace(groups[2])
		n.Rows, _ = strconv.ParseInt(strings.TrimSpace(groups[3]), 10, 64)
		n.Width, _ = strconv.ParseInt(strings.TrimSpace(groups[4]), 10, 64)

	} else {
		fmt.Println("FAIL")
		return errors.New("Unable to parse node")
	}
	
	// Parse the remaining 
	for _, line := range n.RawLines[1:] {
		if patterns["ROWSTAT"].MatchString(line) {
			n.RowStat = parseRowStat(line)
		}
	}

	return nil
}


// ------------------------------------------------------------
// SubPlan 2
//   ->  Limit  (cost=0.00..0.64 rows=1 width=0)
//         ->  Seq Scan on pg_attribute c2  (cost=0.00..71.00 rows=112 width=0)
//               Filter: atttypid = $1
func parsePlan(line string) {
	fmt.Println("PARSE SUBPLAN")

	/*
	   groups := patterns["NODE"].FindStringSubmatch(line)

	   operator := strings.TrimSpace(groups[1])
	   cost := strings.TrimSpace(groups[2])
	   rows, _ := strconv.ParseInt(strings.TrimSpace(groups[3]), 10, 64)
	   width, _ := strconv.ParseInt(strings.TrimSpace(groups[4]), 10, 64)
	   fmt.Printf("\toperator: %s\n\tcost: %s\n\trows: %d\n\twidth: %d\n", operator, cost, rows, width)
	*/

	return
}


// ------------------------------------------------------------
// Settings:  enable_hashjoin=off; enable_indexscan=off; join_collapse_limit=1; optimizer=on
// Settings:  optimizer=off
//
func (e *Explain) parseSettings(line string) {
	fmt.Println("PARSE SETTINGS")
	e.planFinished = true
	line = strings.TrimSpace(line)
	line = line[11:]
	settings := strings.Split(line, "; ")
	for _, setting := range settings {
		temp := strings.Split(setting, "=")
		e.Settings = append(e.Settings, Setting{temp[0], temp[1]})
		fmt.Printf("\t%s\n", setting)
	}
}


// ------------------------------------------------------------
// Slice statistics:
//   (slice0) Executor memory: 2466K bytes.
//   (slice1) Executor memory: 4146K bytes avg x 96 workers, 4146K bytes max (seg7).
//   (slice2) * Executor memory: 153897K bytes avg x 96 workers, 153981K bytes max (seg71). Work_mem: 153588K bytes max, 1524650K bytes wanted.
//
func (e *Explain) parseSliceStats(line string) {
	fmt.Println("PARSE SLICE STATS")
	e.planFinished = true
	for i := e.lineOffset + 1; i < len(e.lines); i++ {
		if getIndent(e.lines[i]) > 1 {
			fmt.Println(e.lines[i])
			e.SliceStats = append(e.SliceStats, strings.TrimSpace(e.lines[i]))
		} else {
			e.lineOffset = i - 1
			break
		}
	}
}


// ------------------------------------------------------------
// Statement statistics:
//   Memory used: 128000K bytes
//   Memory wanted: 1525449K bytes
//
func (e *Explain) parseStatementStats(line string) {
	fmt.Println("PARSE STATEMENT STATS")
	e.planFinished = true
	
	e.StatementStats.MemoryUsed = -1
	e.StatementStats.MemoryWanted = -1

	for i := e.lineOffset + 1; i < len(e.lines); i++ {
		if getIndent(e.lines[i]) > 1 {
			fmt.Println(e.lines[i])
			if patterns["STATEMENTSTATS_USED"].MatchString(e.lines[i]) {
				groups := patterns["STATEMENTSTATS_USED"].FindStringSubmatch(e.lines[i])
				e.StatementStats.MemoryUsed, _ = strconv.ParseInt(strings.TrimSpace(groups[1]), 10, 64)
			} else if patterns["STATEMENTSTATS_WANTED"].MatchString(e.lines[i]) {
				groups := patterns["STATEMENTSTATS_WANTED"].FindStringSubmatch(e.lines[i])
				e.StatementStats.MemoryWanted, _ = strconv.ParseInt(strings.TrimSpace(groups[1]), 10, 64)
			}
		} else {
			e.lineOffset = i - 1
			break
		}
	}
}


// ------------------------------------------------------------
//  Optimizer status: legacy query optimizer
//  Optimizer status: PQO version 1.620
//
func (e *Explain) parseOptimizer(line string) {
	fmt.Println("PARSE OPTIMIZER")
	e.planFinished = true
	line = strings.TrimSpace(line)
	line = line[11:]
	temp := strings.Split(line, ": ")
	e.Optimizer = temp[1]
	fmt.Printf("\t%s\n", e.Optimizer)
}


// ------------------------------------------------------------
// Total runtime: 7442.441 ms
//
func (e *Explain) parseRuntime(line string) {
	fmt.Println("PARSE RUNTIME")
	e.planFinished = true
	line = strings.TrimSpace(line)
	temp := strings.Split(line, " ")
	if s, err := strconv.ParseFloat(temp[2], 64); err == nil {
		e.Runtime = s
	}
	fmt.Printf("\t%f\n", e.Runtime)
}


// Parse all the lines in to empty structs with only RawLines populated
func (e *Explain) ParseLines() {
	fmt.Println("FUNC ParseLines")
	fmt.Printf("Parsing %d lines\n", len(e.lines))
	e.planFinished = false
	// Loop through lines
	for e.lineOffset = 0; e.lineOffset < len(e.lines); e.lineOffset++ {
		fmt.Printf("------------------------------ LINE %d ------------------------------\n", e.lineOffset+1)
		fmt.Println(e.lines[e.lineOffset])
		e.parseline(e.lines[e.lineOffset])
	}
}


func (e *Explain) parseline(line string) {
	indent := getIndent(line)

	// Ignore whitespace, "QUERY PLAN" and "-"
	if len(strings.TrimSpace(line)) == 0 || strings.Index(line, "QUERY PLAN") > -1 || line[:1] == "-" {
		fmt.Println("SKIPPING")
	
	} else if patterns["NODE"].MatchString(line) {
		// Parse a new node
		newNode := parseNode(line)
		
		// Append node to Nodes array
		e.Nodes = append(e.Nodes, newNode)

	} else if patterns["SUBPLAN"].MatchString(line) {
		//newNode := parseNode(line, "NODE")
		parsePlan(line)

	} else if patterns["SLICESTATS"].MatchString(line) {
		e.parseSliceStats(line)

	} else if patterns["STATEMENTSTATS"].MatchString(line) {
		e.parseStatementStats(line)
	} else if patterns["SETTINGS"].MatchString(line) {
		e.parseSettings(line)

	} else if patterns["OPTIMIZER"].MatchString(line) {
		e.parseOptimizer(line)

	} else if patterns["RUNTIME"].MatchString(line) {
		e.parseRuntime(line)

	} else if indent > 1 && e.planFinished == false {
		// Append this line to RawLines on the last node
		e.Nodes[len(e.Nodes)-1].RawLines = append(e.Nodes[len(e.Nodes)-1].RawLines, line)

	} else {
		fmt.Println("SKIPPING")

	}

	return
}


func renderNode(node Node) string {
	OUT := ""
	//OUT += fmt.Sprintf("%s (cost=%s, rows=%d, width=%d)\n", node.Operator, node.Cost, node.Rows, node.Width)
	OUT += fmt.Sprintf("%s\n", node.Operator)
	for _, l := range node.RawLines {
		OUT += fmt.Sprintf(">> %s\n", l)
	}
	//OUT += fmt.Sprintf(">> Children: %d\n", len(node.Children))
	//for _, c := range node.Children {
	//    OUT += renderNode(*c)
	//}

	return OUT
}


func (e *Explain) PrintDebug() {
	fmt.Printf("\n########## START PRINT DEBUG ##########\n")
	for i, node := range e.Nodes {
		//thisIndent := strings.Repeat(" ", node.Indent)
		fmt.Printf("----- %d -----\n", i)
		/*
		fmt.Printf("%s%s | cost %s | rows %d | width %d\n",
			thisIndent,
			node.Operator,
			node.Cost,
			node.Rows,
			node.Width)
		*/
		/*
		fmt.Printf("%sInOut %s | Rows %f | Avg %f | Max %f | Workers %d | First %f | End %f | Offset %f\n",
			thisIndent,
			node.RowStat.InOut,
			node.RowStat.Rows,
			node.RowStat.Avg,
			node.RowStat.Max,
			node.RowStat.Workers,
			node.RowStat.First,
			node.RowStat.End,
			node.RowStat.Offset)
		*/

		for _, line := range node.RawLines {
			fmt.Printf("RAWLINE: %s\n", line)
			//fmt.Printf("%sRAWLINE: %s\n", thisIndent, line)
		}
	}

	fmt.Println("")

	fmt.Println("Slice statistics:")
	for _, stat := range e.SliceStats {
		fmt.Printf("\t%s\n", stat)
	}

	fmt.Println("Statement statistics:")
	fmt.Printf("\tMemory used: %d\n", e.StatementStats.MemoryUsed)
	fmt.Printf("\tMemory wanted: %d\n", e.StatementStats.MemoryWanted)

	fmt.Println("Settings:")
	if len(e.Settings) > 0 {
		for _, setting := range e.Settings {
			fmt.Printf("\t%s=%s\n", setting.Name, setting.Value)
		}
	} else {
		fmt.Printf("\t-\n")
	}

	fmt.Println("Optimizer status:")
	if e.Optimizer != "" {
		fmt.Printf("\t%s\n", e.Optimizer)
	} else {
		fmt.Printf("\t-\n")
	}
	

	fmt.Println("Total runtime:")
	fmt.Printf("\t%f\n", e.Runtime)
	fmt.Printf("########## END PRINT DEBUG ##########\n\n")
}


func (e *Explain) InitFromString(text string) {
	fmt.Printf("FUNCT InitFromString\n")
	// Split the data in to lines
	e.lines = strings.Split(text, "\n")
	e.ParseLines()
}


func (e *Explain) InitFromFile(filename string) error {
	fmt.Printf("FUNC InitFromFile\n")
	// Check file name exists

	// Read all lines
	filedata, err := ioutil.ReadFile(filename)
	check(err)

	// Split the data in to lines
	e.lines = strings.Split(string(filedata), "\n")

	e.ParseLines()

	for _, n := range e.Nodes {
		err := parseNodeRawLines(n)
		if err != nil {
			return err
		}
	}

	return nil
}
