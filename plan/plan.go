package plan

import (
	"os"
	"fmt"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"
	"errors"
	"github.com/pivotal-gss/utils/mlogger"
)

type Node struct {
	// Variables parsed from EXPLAIN
	Operator     string
	Indent       int
	Offset       int
	Slice        int64
	StartupCost  string
	TotalCost    string
	Rows         int64
	Width        int64
	
	// Variable parsed from EXPLAIN ANALYZE
	ActualRows   float64
	AvgRows      float64
	Workers      float64
	MaxRows      float64
	MaxSeg       float64
	Scans        float64
	MsFirst      float64
	MsEnd        float64
	MsOffset     float64
	AvgMem       float64
	MaxMem       float64
	ExecMemLine  float64
	SpillFile    int64
	SpillReuse   int64

	ExtraInfo    []string
	SubNodes     []*Node
	SubPlans     []*Plan
	Warnings     []Warning
}

type Plan struct {
	Name     string
	Indent   int
	Offset   int
	TopNode  *Node
}

type Warning struct {
	Cause       string
	Resolution  string
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
	Plans          []*Plan
	SliceStats     []string
	MemoryUsed     int64
	MemoryWanted   int64
	Settings       []Setting
	Optimizer      string
	Runtime        float64
	Warnings       []Warning

	lines        []string
	lineOffset   int
	planFinished bool
}

var (
	log mlogger.Mlogger

	patterns = map[string]*regexp.Regexp{
		"NODE":               regexp.MustCompile(`(.*) \(cost=(.*)\.\.(.*) rows=(.*) width=(.*)\)`),
		"SLICE":              regexp.MustCompile(`(.*)  \(slice([0-9]*)`),
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

	indentDepth = 4 // Used for printing the plan
	warningColor = 31 // RED
)


// Calculate indent by triming white space and checking diff on string length
func getIndent(line string) int {
	return len(line) - len(strings.TrimLeft(line, " "))
}


// ------------------------------------------------------------
// Checks relating to each node
// ------------------------------------------------------------

// Check Scan nodes to see if estimated rows == 1
func (n *Node) checkNodeEstimatedRows() {
	re := regexp.MustCompile(`(Dynamic Table|Table|Parquet table|Bitmap Index|Bitmap Append-Only Row-Oriented|Seq) Scan`)
	if re.MatchString(n.Operator) {
		if n.Rows == 1 {
			n.Warnings = append(n.Warnings, Warning{
				"Estimated rows is 1",
				"Check if table has been ANALYZED"})
		}
	}
}


// Check for Nested Loops
func (n *Node) checkNodeNestedLoop() {
	re := regexp.MustCompile(`Nested Loop`)
	if re.MatchString(n.Operator) {
		n.Warnings = append(n.Warnings, Warning{
			"Nested Loop",
			"Review query"})
	}
}


func (n *Node) checkNodeSpilling() {
	if n.SpillFile >= 1 {
		n.Warnings = append(n.Warnings, Warning{
			fmt.Sprintf("Total %d spilling segments found", n.SpillFile),
			"Review query"})
	}
}


// ------------------------------------------------------------
// Checks relating to the over all Explain output
// ------------------------------------------------------------

// Check if the number of Broadcast/Redistribute Motion nodes is > 5
func (e *Explain) checkExplainMotionCount() {
	motionCount := 0
	motionCountLimit := 5

	re := regexp.MustCompile(`(Broadcast|Redistribute) Motion`)

	for _, n := range e.Nodes {
		if re.MatchString(n.Operator) {
			motionCount++
		}
	}

	if motionCount > motionCountLimit {
		e.Warnings = append(e.Warnings, Warning{
			fmt.Sprintf("Found %d Redistribute/Broadcast motions", motionCount),
			"Review query"})
	}
}


// Example data to be parsed
//   ->  Hash Join  (cost=0.00..862.00 rows=1 width=16)
//         Hash Cond: public.sales.id = public.sales.year
//         Rows out:  11000 rows (seg0) with 6897 ms to first row, 7429 ms to end, start offset by 40 ms.
//         Executor memory:  127501K bytes avg, 127501K bytes max (seg0).
//         Work_mem used:  127501K bytes avg, 127501K bytes max (seg0). Workfile: (2 spilling, 0 reused)
//         Work_mem wanted: 171875K bytes avg, 171875K bytes max (seg0) to lessen workfile I/O affecting 2 workers.
func parseNodeExtraInfo(n *Node) error {
	// line 0 will always be the node line
	// Example:
	//     ->  Broadcast Motion 1:2  (slice1)  (cost=0.00..27.48 rows=1124 width=208)
	line := n.ExtraInfo[0]

	groups := patterns["NODE"].FindStringSubmatch(line)

	if len(groups) == 6 {
		// Remove the indent arrow
		groups[1] = strings.Trim(groups[1], " ->")

		// Check if the string contains slice information
		sliceGroups := patterns["SLICE"].FindStringSubmatch(groups[1])
		if len(sliceGroups) == 3 {
			n.Operator = strings.TrimSpace(sliceGroups[1])
			n.Slice, _ = strconv.ParseInt(strings.TrimSpace(sliceGroups[2]), 10, 64)
		// Else it's just the operator
		} else {
			n.Operator = strings.TrimSpace(groups[1])
			n.Slice = -1
		}

		// Store the remaining params
		n.StartupCost = strings.TrimSpace(groups[2])
		n.TotalCost = strings.TrimSpace(groups[3])
		n.Rows, _ = strconv.ParseInt(strings.TrimSpace(groups[4]), 10, 64)
		n.Width, _ = strconv.ParseInt(strings.TrimSpace(groups[5]), 10, 64)

	} else {
		return errors.New("Unable to parse node")
	}

	// Init everything to -1
	n.ActualRows   = -1
	n.AvgRows      = -1
	n.Workers      = -1
	n.MaxRows      = -1
	n.MaxSeg       = -1
	n.Scans        = -1
	n.MsFirst      = -1
	n.MsEnd        = -1
	n.MsOffset     = -1
	n.AvgMem       = -1
	n.MaxMem       = -1
	n.ExecMemLine  = -1
	n.SpillFile    = -1
	n.SpillReuse   = -1
	
	// Parse the remaining lines
	var re *regexp.Regexp
	var m []string

	for _, line := range n.ExtraInfo[1:] {
		fmt.Println(line)

		// ROWS
		re = regexp.MustCompile(`ms to end`)
		if re.MatchString(line) {
			re = regexp.MustCompile(`(\d+) rows at destination`)
			m := re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp() + 1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.ActualRows = s
					fmt.Printf("ActualRows %f\n", n.ActualRows)
				}
			}

			re = regexp.MustCompile(`(\d+) rows with \S+ ms`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp() + 1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.ActualRows = s
					fmt.Printf("ActualRows %f\n", n.ActualRows)
				}
			}

			re = regexp.MustCompile(`Max (\S+) rows`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp() + 1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.MaxRows = s
					fmt.Printf("MaxRows %f\n", n.MaxRows)
				}
			}

			re = regexp.MustCompile(` (\S+) ms to first row`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp() + 1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.MsFirst = s
					fmt.Printf("MsFirst %f\n", n.MsFirst)
				}
			}

			re = regexp.MustCompile(` (\S+) ms to end`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp() + 1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.MsEnd = s
					fmt.Printf("MsEnd %f\n", n.MsEnd)
				}
			}

			re = regexp.MustCompile(`start offset by (\S+) ms`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp() + 1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.MsOffset = s
					fmt.Printf("MsOffset %f\n", n.MsOffset)
				}
			}

			re = regexp.MustCompile(`Avg (\S+) `)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp() + 1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.AvgRows = s
					fmt.Printf("AvgRows %f\n", n.AvgRows)
				}
			}

			re = regexp.MustCompile(` x (\d+) workers`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp() + 1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.Workers = s
					fmt.Printf("Workers %f\n", n.Workers)
				}
			}

			re = regexp.MustCompile(`of (\d+) scans`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp() + 1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.Scans = s
					fmt.Printf("Scans %f\n", n.Scans)
				}
			}
		}

		// MEMORY
		re = regexp.MustCompile(`Work_mem used`)
		if re.MatchString(line) {
			re = regexp.MustCompile(`Work_mem used:\s+(\d+)K bytes avg`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp() + 1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.AvgMem = s
					fmt.Printf("AvgMem %f\n", n.AvgMem)
				}
			}

			re = regexp.MustCompile(`\s+(\d+)K bytes max`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp() + 1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.MaxMem = s
					fmt.Printf("MaxMem %f\n", n.MaxMem)
				}
			}
		}

		// SPILL
		re = regexp.MustCompile(`\((\d+) spilling,\s+(\d+) reused\)`)
		m = re.FindStringSubmatch(line)
		if len(m) == re.NumSubexp() + 1 {
			n.SpillFile, _ = strconv.ParseInt(strings.TrimSpace(m[1]), 10, 64)
			n.SpillReuse, _ = strconv.ParseInt(strings.TrimSpace(m[2]), 10, 64)
			fmt.Printf("SpillFile %d\n", n.SpillFile)
			fmt.Printf("SpillReuse %d\n", n.SpillReuse)
		}

		// #Executor memory:  4978K bytes avg, 39416K bytes max (seg2).
		// if ( $info_line =~ m/Executor memory:/ ) {
		//     $exec_mem_line .= $info_line."\n";
		// }

	}

	return nil
}


func (e *Explain) createNode(line string) *Node {
	log.Debugf("createNode\n")
	// Set node indent
	// Rest of node parsing is handled in parseNodeExtraInfo
	node := new(Node)
	node.Indent = getIndent(line)
	node.Offset = e.lineOffset
	node.ExtraInfo = []string{
		line,
	}

	return node
}


// ------------------------------------------------------------
// SubPlan 2
//   ->  Limit  (cost=0.00..0.64 rows=1 width=0)
//         ->  Seq Scan on pg_attribute c2  (cost=0.00..71.00 rows=112 width=0)
//               Filter: atttypid = $1
func (e *Explain) createPlan(line string) *Plan {
	log.Debugf("createPlan\n")

	plan := new(Plan)
	plan.Name = strings.Trim(line, " ")
	plan.Indent = getIndent(line)
	plan.Offset = e.lineOffset
	plan.TopNode = new(Node)

	return plan
}


// ------------------------------------------------------------
// Settings:  enable_hashjoin=off; enable_indexscan=off; join_collapse_limit=1; optimizer=on
// Settings:  optimizer=off
//
func (e *Explain) parseSettings(line string) {
	log.Debugf("parseSettings\n")
	e.planFinished = true
	line = strings.TrimSpace(line)
	line = line[11:]
	settings := strings.Split(line, "; ")
	for _, setting := range settings {
		temp := strings.Split(setting, "=")
		e.Settings = append(e.Settings, Setting{temp[0], temp[1]})
		log.Debugf("\t%s\n", setting)
	}
}


// ------------------------------------------------------------
// Slice statistics:
//   (slice0) Executor memory: 2466K bytes.
//   (slice1) Executor memory: 4146K bytes avg x 96 workers, 4146K bytes max (seg7).
//   (slice2) * Executor memory: 153897K bytes avg x 96 workers, 153981K bytes max (seg71). Work_mem: 153588K bytes max, 1524650K bytes wanted.
//
func (e *Explain) parseSliceStats(line string) {
	log.Debugf("parseSliceStats\n")
	e.planFinished = true
	for i := e.lineOffset + 1; i < len(e.lines); i++ {
		if getIndent(e.lines[i]) > 1 {
			log.Debugf("%s\n", e.lines[i])
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
	log.Debugf("parseStatementStats\n")
	e.planFinished = true
	
	e.MemoryUsed = -1
	e.MemoryWanted = -1

	for i := e.lineOffset + 1; i < len(e.lines); i++ {
		if getIndent(e.lines[i]) > 1 {
			log.Debugf(e.lines[i])
			if patterns["STATEMENTSTATS_USED"].MatchString(e.lines[i]) {
				groups := patterns["STATEMENTSTATS_USED"].FindStringSubmatch(e.lines[i])
				e.MemoryUsed, _ = strconv.ParseInt(strings.TrimSpace(groups[1]), 10, 64)
			} else if patterns["STATEMENTSTATS_WANTED"].MatchString(e.lines[i]) {
				groups := patterns["STATEMENTSTATS_WANTED"].FindStringSubmatch(e.lines[i])
				e.MemoryWanted, _ = strconv.ParseInt(strings.TrimSpace(groups[1]), 10, 64)
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
	log.Debugf("PARSE OPTIMIZER\n")
	e.planFinished = true
	line = strings.TrimSpace(line)
	line = line[11:]
	temp := strings.Split(line, ": ")
	e.Optimizer = temp[1]
	log.Debugf("\t%s\n", e.Optimizer)
}


// ------------------------------------------------------------
// Total runtime: 7442.441 ms
//
func (e *Explain) parseRuntime(line string) {
	log.Debugf("PARSE RUNTIME\n")
	e.planFinished = true
	line = strings.TrimSpace(line)
	temp := strings.Split(line, " ")
	if s, err := strconv.ParseFloat(temp[2], 64); err == nil {
		e.Runtime = s
	}
	log.Debugf("\t%f\n", e.Runtime)
}


// Parse all the lines in to empty structs with only ExtraInfo populated
func (e *Explain) parseLines() {
	log.Debugf("ParseLines\n")
	log.Debugf("Parsing %d lines\n", len(e.lines))
	e.planFinished = false
	// Loop through lines
	for e.lineOffset = 0; e.lineOffset < len(e.lines); e.lineOffset++ {
		log.Debugf("------------------------------ LINE %d ------------------------------\n", e.lineOffset+1)
		log.Debugf("%s\n", e.lines[e.lineOffset])
		e.parseline(e.lines[e.lineOffset])
	}
}


func (e *Explain) parseline(line string) {
	indent := getIndent(line)

	// Ignore whitespace, "QUERY PLAN" and "-"
	if len(strings.TrimSpace(line)) == 0 || strings.Index(line, "QUERY PLAN") > -1 || line[:1] == "-" {
		log.Debugf("SKIPPING\n")
	
	} else if patterns["NODE"].MatchString(line) {
		// Parse a new node
		newNode := e.createNode(line)

		// If this is the first node then insert the TopPlan also
		if len(e.Nodes) == 0 {
			newPlan := e.createPlan("Plan")
			e.Plans = append(e.Plans, newPlan)
		}
		
		// Append node to Nodes array
		e.Nodes = append(e.Nodes, newNode)

	} else if patterns["SUBPLAN"].MatchString(line) {
		// Parse a new plan
		newPlan := e.createPlan(line)

		// Append plan to Plans array
		e.Plans = append(e.Plans, newPlan)

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
		// Append this line to ExtraInfo on the last node
		e.Nodes[len(e.Nodes)-1].ExtraInfo = append(e.Nodes[len(e.Nodes)-1].ExtraInfo, line)

	} else {
		log.Debugf("SKIPPING\n")

	}

	return
}


func (n *Node) renderNode(node Node) string {
	OUT := ""
	//OUT += fmt.Sprintf("%s (cost=%s, rows=%d, width=%d)\n", node.Operator, node.Cost, node.Rows, node.Width)
	OUT += fmt.Sprintf("%s\n", n.Operator)
	for _, l := range n.ExtraInfo {
		OUT += fmt.Sprintf(">> %s\n", l)
	}
	//OUT += fmt.Sprintf(">> Children: %d\n", len(node.Children))
	//for _, c := range node.Children {
	//    OUT += renderNode(*c)
	//}

	return OUT
}


func (e *Explain) BuildTree() {
	log.Debugf("########## START BUILD TREE ##########\n")

	// Walk backwards through the Plans array and a
	log.Debugf("########## PLANS ##########\n")
	for i := len(e.Plans)-1; i > -1; i-- {
		log.Debugf("%d %s\n", e.Plans[i].Indent, e.Plans[i].Name)

		// Loop upwards to find parent
		for p := len(e.Nodes)-1; p > -1; p-- {
			log.Debugf("\t%d %s\n", e.Nodes[p].Indent, e.Nodes[p].Operator)
			if e.Plans[i].Indent > e.Nodes[p].Indent && e.Plans[i].Offset > e.Nodes[p].Offset {
				log.Debugf("\t\tFOUND PARENT NODE\n")
				// Prepend to start of array to keep ordering
				e.Nodes[p].SubPlans = append([]*Plan{e.Plans[i]}, e.Nodes[p].SubPlans...)
				break
			}
		}
	}

	// Insert Nodes
	log.Debugf("########## NODES ##########\n")
	for i := len(e.Nodes)-1; i > -1; i-- {
		log.Debugf("%d %s\n", e.Nodes[i].Indent, e.Nodes[i].Operator)

		foundParent := false

		// Loop upwards to find parent

		// First check for parent plans
		for p := len(e.Plans)-1; p > -1; p-- {
			log.Debugf("\t%d %s\n", e.Plans[p].Indent, e.Plans[p].Name)
			// If the parent is a SubPlan it will always be Indent-2 and Offset-1
			//  SubPlan 1
			//    ->  Limit  (cost=0.00..9.23 rows=1 width=0)
			if (e.Nodes[i].Indent - 2) == e.Plans[p].Indent && (e.Nodes[i].Offset -1) == e.Plans[p].Offset {
				log.Debugf("\t\tFOUND PARENT PLAN\n")
				// Prepend to start of array to keep ordering
				e.Plans[p].TopNode = e.Nodes[i]
				foundParent = true
				break
			}
		}

		if foundParent == true {
			continue
		}

		foundParent = false

		// Then check for parent nodes
		for p := i -1; p > -1; p-- {
			log.Debugf("\t%d %s\n", e.Nodes[p].Indent, e.Nodes[p].Operator)
			if e.Nodes[i].Indent > e.Nodes[p].Indent {
				log.Debugf("\t\tFOUND PARENT NODE\n")
				// Prepend to start of array to keep ordering
				e.Nodes[p].SubNodes = append([]*Node{e.Nodes[i]}, e.Nodes[p].SubNodes...)
				foundParent = true
				break
			}
		}

		// 
		if foundParent == false {
			log.Debugf("\t\tTOPNODE\n")
			e.Plans[0].TopNode = e.Nodes[i]
		}
	}

	log.Debugf("########## END BUILD TREE ##########\n")
}


func (n *Node) Render(indent int) {
	indent += 1
	indentString := strings.Repeat(" ", indent * indentDepth)
	
	if n.Slice > -1 {
		fmt.Printf("\n%s   // Slice %d\n", indentString, n.Slice)
	}

	fmt.Printf("%s-> %s | startup cost %s | total cost %s | rows %d | width %d\n",
			indentString,
			n.Operator,
			n.StartupCost,
			n.TotalCost,
			n.Rows,
			n.Width)

	// Render ExtraInfo
	for _, e := range n.ExtraInfo[1:] {
		fmt.Printf("%s   %s\n", indentString, strings.Trim(e, " "))
	}

	// Render warnings
	for _, w := range n.Warnings {
		fmt.Printf("\x1b[%dm", warningColor)
		fmt.Printf("%s   WARNING: %s | %s\n", indentString, w.Cause, w.Resolution)
		fmt.Printf("\x1b[%dm", 0)
	}

	// Render sub nodes
	for _, s := range n.SubNodes {
		s.Render(indent)
	}

	// Render sub plans
	for _, s := range n.SubPlans {
		s.Render(indent)
	}
}


func (p *Plan) Render(indent int) {
	indent += 1
	indentString := strings.Repeat(" ", indent * indentDepth)

	fmt.Printf("%s%s\n", indentString, p.Name)
	p.TopNode.Render(indent)
}


func (n *Node) RenderHtml(indent int) string {
	HTML := ""
	indent += 1
	indentString := strings.Repeat(" ", indent * indentDepth)
	
	if n.Slice > -1 {
		HTML += fmt.Sprintf("%s   <span class=\"label label-success\">Slice %d</span>\n",
			indentString,
			n.Slice)
	}
	HTML += fmt.Sprintf("%s<strong>-> %s | startup cost %s | total cost %s | rows %d | width %d</strong>\n",
			indentString,
			n.Operator,
			n.StartupCost,
			n.TotalCost,
			n.Rows,
			n.Width)

	for _, e := range n.ExtraInfo[1:] {
		HTML += fmt.Sprintf("%s   %s\n", indentString, strings.Trim(e, " "))
	}

	for _, w := range n.Warnings {
		HTML += fmt.Sprintf("%s   <span class=\"label label-danger\">WARNING: %s | %s</span>\n", indentString, w.Cause, w.Resolution)
	}
	// Render sub nodes
	for _, s := range n.SubNodes {
		HTML += s.RenderHtml(indent)
	}

	for _, s := range n.SubPlans {
		HTML += s.RenderHtml(indent)
	}

	return HTML
}


func (p *Plan) RenderHtml(indent int) string {
	HTML := ""
	indent += 1
	indentString := strings.Repeat(" ", indent * indentDepth)

	HTML += fmt.Sprintf("%s<strong>%s</strong>", indentString, p.Name)
	HTML += p.TopNode.RenderHtml(indent)
	return HTML
}


func (e *Explain) PrintPlan() {

	fmt.Println("Plan:")
	e.Plans[0].TopNode.Render(0)
	
	/*
		if node.Slice > -1 {
			fmt.Printf("%sSLICE: slice %d\n",
				thisIndent,
				node.Slice)
		}

		for _, n := range node.SubNodes {
			fmt.Printf("%sSUBNODE: %s\n", thisIndent, n.Operator)
		}

		for _, p := range node.SubPlans {
			fmt.Printf("%sSUBPLAN: %s\n", thisIndent, p.Name)
		}
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

		/*
		for _, line := range node.ExtraInfo {
			fmt.Printf("%sRAWLINE: %s\n", thisIndent, strings.Trim(line, " "))
		}
		*/
	
	if len(e.Warnings) > 0 {
		fmt.Printf("\n")
		for _, w := range e.Warnings {
			fmt.Printf("\x1b[%dm", warningColor)
			fmt.Printf("WARNING: %s | %s\n", w.Cause, w.Resolution)
			fmt.Printf("\x1b[%dm", 0)
		}
	}

	fmt.Printf("\n")

	if len(e.SliceStats) > 0 {
		fmt.Println("Slice statistics:")
		for _, stat := range e.SliceStats {
			fmt.Printf("\t%s\n", stat)
		}
	}

	if e.MemoryUsed > 0 {
		fmt.Println("Statement statistics:")
		fmt.Printf("\tMemory used: %d\n", e.MemoryUsed)
		fmt.Printf("\tMemory wanted: %d\n", e.MemoryWanted)
	}
	
	if len(e.Settings) > 0 {
		fmt.Println("Settings:")
		for _, setting := range e.Settings {
			fmt.Printf("\t%s = %s\n", setting.Name, setting.Value)
		}
	}

	if e.Optimizer != "" {
		fmt.Println("Optimizer status:")
		fmt.Printf("\t%s\n", e.Optimizer)
	}
	
	if e.Runtime > 0 {
		fmt.Println("Total runtime:")
		fmt.Printf("\t%.0f ms\n", e.Runtime)
	}

}


func (e *Explain) PrintPlanHtml() string {
	HTML := ""
	HTML += fmt.Sprintf("<strong>Plan:</strong>\n")
	HTML += e.Plans[0].TopNode.RenderHtml(0)
	
	/*
		if node.Slice > -1 {
			fmt.Printf("%sSLICE: slice %d\n",
				thisIndent,
				node.Slice)
		}

		for _, n := range node.SubNodes {
			fmt.Printf("%sSUBNODE: %s\n", thisIndent, n.Operator)
		}

		for _, p := range node.SubPlans {
			fmt.Printf("%sSUBPLAN: %s\n", thisIndent, p.Name)
		}
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

		/*
		for _, line := range node.ExtraInfo {
			fmt.Printf("%sRAWLINE: %s\n", thisIndent, strings.Trim(line, " "))
		}
		*/

	if len(e.Warnings) > 0 {
		HTML += fmt.Sprintf("<strong>Warnings:</strong>\n")
		for _, w := range e.Warnings {
			HTML += fmt.Sprintf("\t<span class=\"label label-danger\">%s | %s</span>\n", w.Cause, w.Resolution)
		}
	}

	if len(e.SliceStats) > 0 {
		HTML += fmt.Sprintf("<strong>Slice statistics:</strong>\n")
		for _, stat := range e.SliceStats {
			HTML += fmt.Sprintf("\t%s\n", stat)
		}
	}

	if e.MemoryUsed > 0 {
		HTML += fmt.Sprintf("<strong>Statement statistics:</strong>\n")
		HTML += fmt.Sprintf("\tMemory used: %d\n", e.MemoryUsed)
		HTML += fmt.Sprintf("\tMemory wanted: %d\n", e.MemoryWanted)
	}
	
	if len(e.Settings) > 0 {
		HTML += fmt.Sprintf("<strong>Settings:</strong>\n")
		for _, setting := range e.Settings {
			HTML += fmt.Sprintf("\t%s = %s\n", setting.Name, setting.Value)
		}
	}

	if e.Optimizer != "" {
		HTML += fmt.Sprintf("<strong>Optimizer status:</strong>\n")
		HTML += fmt.Sprintf("\t%s\n", e.Optimizer)
	}
	
	if e.Runtime > 0 {
		HTML += fmt.Sprintf("<strong>Total runtime:</strong>\n")
		HTML += fmt.Sprintf("\t%.0f ms\n", e.Runtime)
	}

	return HTML
}


func (e *Explain) InitLogger(debug bool) error {
	var err error
	log, err = mlogger.NewStdoutOnlyLogger()
	if err != nil {
		return err
	}

	if debug == true {
		log.EnableDebug()
	}

	return nil
}


func (e *Explain) InitPlan(plantext string) error {

	// Split the data in to lines
	e.lines = strings.Split(string(plantext), "\n")

	// Parse lines in to node objects
	e.parseLines()

	if len(e.Nodes) == 0 {
		return errors.New("Could not find any nodes in plan")
	}

	// Convert array of nodes to tree structure
	e.BuildTree()

	for _, n := range e.Nodes {
		// Parse ExtraInfo
		err := parseNodeExtraInfo(n)
		if err != nil {
			return err
		}

		// Run Node checks
		n.checkNodeEstimatedRows()
		n.checkNodeNestedLoop()
		n.checkNodeSpilling()
	}

	// Run Explain checks
	e.checkExplainMotionCount()

	return nil
}


func (e *Explain) InitFromStdin(debug bool) error {
	e.InitLogger(debug)

	log.Debugf("InitFromStdin\n")

	fi, err := os.Stdin.Stat()
	if err != nil {
		panic(err)
	}

	if fi.Size() == 0 {
		return errors.New("stdin is empty")
	}

	bytes, _ := ioutil.ReadAll(os.Stdin)
	plantext := string(bytes)

	e.InitPlan(plantext)

	return nil
}


func (e *Explain) InitFromString(plantext string, debug bool) error {
	e.InitLogger(debug)

	log.Debugf("InitFromString\n")

	err := e.InitPlan(plantext)
	if err != nil {
		return err
	}
	
	return nil
}


func (e *Explain) InitFromFile(filename string, debug bool) error {
	e.InitLogger(debug)

	log.Debugf("InitFromFile\n")

	// Check file exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return err
	}

	// Read all lines
	filedata, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	plantext := string(filedata)

	err = e.InitPlan(plantext)
	if err != nil {
		return err
	}

	return nil
}
