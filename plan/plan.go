package plan

import (
	"errors"
	"fmt"
	"github.com/pivotal-gss/utils/mlogger"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// Represents a node (anything indented with "->" in the plan)
type Node struct {
	// Location in the file used to build the tree
	Indent int
	Offset int

	// Variables parsed from EXPLAIN
	Operator    string
	Object      string // Name of index or table. Only exists for some nodes
	ObjectType  string // TABLE, INDEX, etc...
	Slice       int64
	StartupCost float64
	TotalCost   float64
	NodeCost    float64
	PrctCost    float64
	Rows        int64
	Width       int64

	// Variables parsed from EXPLAIN ANALYZE
	ActualRows   float64
	AvgRows      float64
	Workers      int64
	MaxRows      float64
	MaxSeg       string
	Scans        int64
	MsFirst      float64
	MsEnd        float64
	MsOffset     float64
	MsNode       float64
	MsPrct       float64
	AvgMem       float64
	MaxMem       float64
	ExecMemLine  float64
	SpillFile    int64
	SpillReuse   int64
	PartSelected int64
	PartTotal    int64
	Filter       string

	// Contains all the text lines below each node
	ExtraInfo []string

	// Populated in BuildTree() to link nodes/plans together
	SubNodes []*Node
	SubPlans []*Plan

	// Populated with any warning for the node
	Warnings []Warning

	// Flag to detect if we are looking at EXPLAIN or EXPLAIN ANALYZE output
	IsAnalyzed bool
}

// Each plan has a top node
type Plan struct {
	Name    string
	Indent  int
	Offset  int
	TopNode *Node
}

// Warnings get added to the overall Explain object or a Node object
type Warning struct {
	Cause      string // What caused the warning
	Resolution string // What should be done to resolve it
}

type NodeCheck struct {
	Name        string
	Description string
	CreatedAt   string
	Scope       []string
	Exec        func(*Node)
}

type ExplainCheck struct {
	Name        string
	Description string
	CreatedAt   string
	Scope       []string
	Exec        func(*Explain)
}

// Slice stats parsed from EXPLAIN ANALYZE output
type SliceStat struct {
	Name          string
	MemoryAvg     int64
	Workers       int64
	MemoryMax     int64
	WorkMem       int64
	WorkMemWanted int64
}

// GUCs are parsed so can do checks for specific settings
type Setting struct {
	Name  string
	Value string
}

// Top level object
type Explain struct {
	Nodes        []*Node // All nodes get added here
	Plans        []*Plan // All plans get added here
	SliceStats   []string
	MemoryUsed   int64
	MemoryWanted int64
	Settings     []Setting
	Optimizer    string
	OptimizerStatus string
	Runtime      float64

	// Populated with any warning for the overall EXPLAIN output
	Warnings []Warning

	lines        []string
	lineOffset   int
	planFinished bool
}

var (
	log mlogger.Mlogger

	patterns = map[string]*regexp.Regexp{
		"NODE":    regexp.MustCompile(`(.*) \((cost=(.*)\.\.(.*) ){0,1}rows=(.*) width=(.*)\)`),
		"SLICE":   regexp.MustCompile(`(.*)  \(slice([0-9]*)`),
		"SUBPLAN": regexp.MustCompile(` SubPlan `),

		"SLICESTATS":   regexp.MustCompile(` Slice statistics:`),
		"SLICESTATS_1": regexp.MustCompile(`\((slice[0-9]{1,})\).*Executor memory: ([0-9]{1,})K bytes`),
		"SLICESTATS_2": regexp.MustCompile(`avg x ([0-9]+) workers, ([0-9]+)K bytes max \((seg[0-9]+)\)\.`),
		"SLICESTATS_3": regexp.MustCompile(`Work_mem: ([0-9]+)K bytes max.`),
		"SLICESTATS_4": regexp.MustCompile(`([0-9]+)K bytes wanted.`),

		"STATEMENTSTATS":        regexp.MustCompile(` Statement statistics:`),
		"STATEMENTSTATS_USED":   regexp.MustCompile(`Memory used: ([0-9.-]{1,})K bytes`),
		"STATEMENTSTATS_WANTED": regexp.MustCompile(`Memory wanted: ([0-9.-]{1,})K bytes`),

		"SETTINGS":  regexp.MustCompile(` Settings: `),
		"OPTIMIZER": regexp.MustCompile(` Optimizer status: `),
		"RUNTIME":   regexp.MustCompile(` Total runtime: `),
	}

	// Keep all checks in NODECHEKS and EXPLAINCHECKS so that we can
	// dynamically create a list of checks to display in webapp
	
	// ------------------------------------------------------------
	// Checks relating to each node
	// ------------------------------------------------------------
	NODECHECKS = []NodeCheck{
		NodeCheck{
			"checkNodeEstimatedRows",
			"Scan node with estimated rows equal to 1",
			"2016-05-24",
			[]string{"orca", "legacy"},
			func (n *Node) {
				re := regexp.MustCompile(`(Dynamic Table|Table|Parquet table|Bitmap Index|Bitmap Append-Only Row-Oriented|Seq) Scan`)
				if re.MatchString(n.Operator) {
					if n.Rows == 1 {
						warningAction := ""
						// Preformat the string here
						if n.ObjectType == "TABLE" {
							warningAction = fmt.Sprintf("ANALYZE on table")
						} else if n.ObjectType == "INDEX" {
							warningAction = fmt.Sprintf("REINDEX on index")
						}

						// If EXPLAIN ANALYZE output then have to check further
						if n.IsAnalyzed == true {
							if n.ActualRows > 1 || n.AvgRows > 1 {
								n.Warnings = append(n.Warnings, Warning{
									"Actual rows is higher than estimated rows",
									fmt.Sprintf("Need to run %s \"%s\"", warningAction, n.Object)})
							}
							// Else just flag as a potential not analyzed table
						} else {
							n.Warnings = append(n.Warnings, Warning{
								"Estimated rows is 1",
								fmt.Sprintf("May need to run %s \"%s\"", warningAction, n.Object)})
						}
					}
				}
			}},
		NodeCheck{
			"checkNodeNestedLoop",
			"Nested Loops",
			"2016-05-23",
			[]string{"orca", "legacy"},
			func (n *Node) {
				re := regexp.MustCompile(`Nested Loop`)
				if re.MatchString(n.Operator) {
					n.Warnings = append(n.Warnings, Warning{
						"Nested Loop",
						"Review query"})
				}
			}},
		NodeCheck{
			"checkNodeSpilling",
			"Spill files",
			"2016-05-31",
			[]string{"orca", "legacy"},
			func (n *Node) {
				if n.SpillFile >= 1 {
					n.Warnings = append(n.Warnings, Warning{
						fmt.Sprintf("Total %d spilling segments found", n.SpillFile),
						"Review query"})
				}
			}},
		NodeCheck{
			"checkNodeScans",
			"Node looping multiple times",
			"2016-05-31",
			[]string{"orca", "legacy"},
			func (n *Node) {
				if n.Scans > 1 {
					n.Warnings = append(n.Warnings, Warning{
						fmt.Sprintf("This node is executed %d times", n.Scans),
						"Review query"})
				}
			}},
		NodeCheck{
			"checkNodePartitionScans",
			"Number of partition scans greater than 100 or 25%%",
			"2016-05-31",
			[]string{"orca", "legacy"},
			func (n *Node) {
				partitionThreshold := int64(100)
				partitionPrctThreshold := int64(25)

				// Planner
				re := regexp.MustCompile(`Append`)
				if re.MatchString(n.Operator) {
					// Warn if the Append node has more than 100 subnodes
					if int64(len(n.SubNodes)) >= partitionThreshold {
						n.Warnings = append(n.Warnings, Warning{
							fmt.Sprintf("Detected %d partition scans", len(n.SubNodes)),
							"Check if partitions can be eliminated"})
					}
				}

				// ORCA
				re = regexp.MustCompile(`Partition Selector`)
				if re.MatchString(n.Operator) {
					// Warn if selected partitions is great than 100
					if n.PartSelected >= partitionThreshold {
						n.Warnings = append(n.Warnings, Warning{
							fmt.Sprintf("Detected %d partition scans", n.PartSelected),
							"Check if partitions can be eliminated"})
					}

					// Warn if selected partitons is 0, may be an issue
					if n.PartSelected == 0 {
						n.Warnings = append(n.Warnings, Warning{
							"Zero partitions selected",
							"Review query"})
						// Also warn if greater than 25% of total partitions were selected.
						// I just chose 25% for now... may need to be adjusted to a more reasonable value
					} else if (n.PartSelected * 100 / n.PartTotal) >= partitionPrctThreshold {
						n.Warnings = append(n.Warnings, Warning{
							fmt.Sprintf("%d%% (%d out of %d) partitions selected", (n.PartSelected * 100 / n.PartTotal), n.PartSelected, n.PartTotal),
							"Check if partitions can be eliminated"})
					}
				}
			}},
		NodeCheck{
			"checkNodeDataSkew",
			"Data skew",
			"2016-06-02",
			[]string{"orca", "legacy"},
			func (n *Node) {
				threshold := 10000.0

				// Only proceed if over threshold
				if n.ActualRows >= threshold || n.AvgRows >= threshold {
					// Handle AvgRows
					if n.AvgRows > 0 {
						// A segment has more than 50% of all rows
						// Only do this if workers > 2 otherwise this situation will report skew:
						//     Rows out:  Avg 500000.0 rows x 2 workers.  Max 500001 rows (seg0)
						// but seg0 only has 1 extra row
						if (n.MaxRows > (n.AvgRows * float64(n.Workers) / 2.0)) && n.Workers > 2 {
							n.Warnings = append(n.Warnings, Warning{
								fmt.Sprintf("Data skew on segment %s", n.MaxSeg),
								"Review query"})
						}
						// Handle ActualRows
						// If ActualRows is set and MaxSeg is set then this
						// segment has the highest rows
					} else if n.ActualRows > 0 && n.MaxSeg != "-" {
						n.Warnings = append(n.Warnings, Warning{
							fmt.Sprintf("Data skew on segment %s", n.MaxSeg),
							"Review query"})
					}
				}
			}},
		NodeCheck{
			"checkNodeFilterWithFunction",
			"Filter clause using function",
			"2016-06-06",
			[]string{"orca", "legacy"},
			// Example:
			//     upper(brief_status::text) = ANY ('{SIGNED,BRIEF,PROPO}'::text[])
			//
			func (n *Node) {
				re := regexp.MustCompile(`\S+\(.*\) `)

				if re.MatchString(n.Filter) {
					n.Warnings = append(n.Warnings, Warning{
						"Filter using function",
						"Check if function can be avoided"})
				}
			}},
	}

	// ------------------------------------------------------------
	// Checks relating to the over all Explain output
	// ------------------------------------------------------------
	EXPLAINCHECKS = []ExplainCheck{
		ExplainCheck{
			"checkExplainMotionCount",
			"Number of Broadcast/Redistribute Motion nodes greater than 5",
			"2016-05-23",
			[]string{"orca", "legacy"},
			func (e *Explain) {
				motionCount := 0
				motionCountLimit := 5

				re := regexp.MustCompile(`(Broadcast|Redistribute) Motion`)

				for _, n := range e.Nodes {
					if re.MatchString(n.Operator) {
						motionCount++
					}
				}

				if motionCount >= motionCountLimit {
					e.Warnings = append(e.Warnings, Warning{
						fmt.Sprintf("Found %d Redistribute/Broadcast motions", motionCount),
						"Review query"})
				}
			}},
		ExplainCheck{
			"checkExplainSliceCount",
			"Number of slices greater than 100",
			"2016-05-31",
			[]string{"orca", "legacy"},
			func (e *Explain) {
				sliceCount := 0
				sliceCountLimit := 100

				for _, n := range e.Nodes {
					if n.Slice > -1 {
						sliceCount++
					}
				}

				if sliceCount > sliceCountLimit {
					e.Warnings = append(e.Warnings, Warning{
						fmt.Sprintf("Found %d slices", sliceCount),
						"Review query"})
				}
			}},
		ExplainCheck{
			"checkExplainPlannerFallback",
			"ORCA fallback to legacy query planner",
			"2016-05-31",
			[]string{"orca"},
			func (e *Explain) {
				// Settings:  optimizer=on
				// Optimizer status: legacy query optimizer
				re := regexp.MustCompile(`legacy query optimizer`)

				if re.MatchString(e.OptimizerStatus) {
					for _, s := range e.Settings {
						if s.Name == "optimizer" && s.Value == "on" {
							e.Warnings = append(e.Warnings, Warning{
								"ORCA enabled but plan was produced by legacy query optimizer",
								"No Action Required"})
							break
						}
					}
				}
			}},
		ExplainCheck{
			"checkExplainEnableGucNonDefault",
			"\"enable_\" GUCs configured with non-default values",
			"2016-06-06",
			[]string{"orca", "legacy"},
			func (e *Explain) {
				// Default GUC values.
				// http://gpdb.docs.pivotal.io/4340/guc_config-topic3.html
				defaults := map[string]string{
					"enable_bitmapscan": "on",
					"enable_groupagg": "on",
					"enable_hashagg": "on",
					"enable_hashjoin": "on",
					"enable_indexscan": "on",
					"enable_seqscan": "on",
					"enable_sort": "on",
					"enable_tidscan": "on",
					"enable_nestloop": "off",
					"enable_mergejoin": "off",
				}

				// Settings:  enable_hashjoin=off; enable_indexscan=off; join_collapse_limit=1; optimizer=on
				re := regexp.MustCompile(`enable_`)

				for _, s := range e.Settings {
					if re.MatchString(s.Name) {
						if value, ok := defaults[s.Name]; ok {
							// Only report if NOT default value
							if s.Value != value {
								e.Warnings = append(e.Warnings, Warning{
									fmt.Sprintf("\"%s\" GUC has non-default value \"%s\"", s.Name, s.Value),
									fmt.Sprintf("Check if \"%s\" GUC is required", s.Name)})
							}
						}
					}
				}
			}},
		ExplainCheck{
			"checkExplainOrcaChildPartitionScan",
			"Scan on child partition instead of root partition",
			"2016-06-08",
			[]string{"orca"},
			func (e *Explain) {

				// Skip if using legacy
				if e.Optimizer != "on" {
					return
				}

				// ->  Seq Scan on sales_1_prt_outlying_years s  (cost=0.00..55276.72 rows=2476236 width=8)
				// ->  Seq Scan on sales_1_prt_2 s  (cost=0.00..38.44 rows=1722 width=8)
				re := regexp.MustCompile(`_[0-9]+_prt_`)

				for _, n := range e.Nodes {
					// Check if object name looks like partition
					if re.MatchString(n.Operator) {
						n.Warnings = append(n.Warnings, Warning{
							fmt.Sprintf("Scan on what appears to be a child partition"),
							fmt.Sprintf("Recommend using root partition when ORCA is enabled")})
					}
				}
			}},
	}

	indentDepth  = 4  // Used for printing the plan
	warningColor = 31 // RED
)

// Calculate indent by triming white space and checking diff on string length
func getIndent(line string) int {
	return len(line) - len(strings.TrimLeft(line, " "))
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

	n.Object = ""
	n.ObjectType = ""

	if len(groups) == 7 {
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

		// Try to get object name if this is a scan node
		// Look for non index scans
		re := regexp.MustCompile(`(Index ){0,0} Scan (on|using) (\S+)`)
		temp := re.FindStringSubmatch(n.Operator)
		if len(temp) == re.NumSubexp()+1 {
			n.Object = temp[3]
			n.ObjectType = "TABLE"
		}

		// Look for index scans
		re = regexp.MustCompile(`Index.*Scan (on|using) (\S+)`)
		temp = re.FindStringSubmatch(n.Operator)
		if len(temp) == re.NumSubexp()+1 {
			n.Object = temp[2]
			n.ObjectType = "INDEX"
		}

		// Store the remaining params
		n.StartupCost, _ = strconv.ParseFloat(strings.TrimSpace(groups[3]), 64)
		n.TotalCost, _ = strconv.ParseFloat(strings.TrimSpace(groups[4]), 64)
		n.Rows, _ = strconv.ParseInt(strings.TrimSpace(groups[5]), 10, 64)
		n.Width, _ = strconv.ParseInt(strings.TrimSpace(groups[6]), 10, 64)

	} else {
		return errors.New("Unable to parse node")
	}

	// Init everything to -1
	n.ActualRows = -1
	n.AvgRows = -1
	n.Workers = -1
	n.MaxRows = -1
	n.MaxSeg = "-"
	n.Scans = -1
	n.MsFirst = -1
	n.MsEnd = -1
	n.MsOffset = -1
	n.AvgMem = -1
	n.MaxMem = -1
	n.ExecMemLine = -1
	n.SpillFile = -1
	n.SpillReuse = -1
	n.PartSelected = -1
	n.PartTotal = -1
	n.Filter = ""
	n.IsAnalyzed = false

	// Parse the remaining lines
	var re *regexp.Regexp
	var m []string

	for _, line := range n.ExtraInfo[1:] {
		log.Debugf("%s\n", line)

		// ROWS
		re = regexp.MustCompile(`ms to end`)
		if re.MatchString(line) {
			n.IsAnalyzed = true
			re = regexp.MustCompile(`(\d+) rows at destination`)
			m := re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.ActualRows = s
					log.Debugf("ActualRows %f\n", n.ActualRows)
				}
			}

			re = regexp.MustCompile(`(\d+) rows with \S+ ms`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.ActualRows = s
					log.Debugf("ActualRows %f\n", n.ActualRows)
				}
			}

			re = regexp.MustCompile(`Max (\S+) rows`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.MaxRows = s
					log.Debugf("MaxRows %f\n", n.MaxRows)
				}
			}

			re = regexp.MustCompile(` (\S+) ms to first row`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.MsFirst = s
					log.Debugf("MsFirst %f\n", n.MsFirst)
				}
			}

			re = regexp.MustCompile(` (\S+) ms to end`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.MsEnd = s
					log.Debugf("MsEnd %f\n", n.MsEnd)
				}
			}

			re = regexp.MustCompile(`start offset by (\S+) ms`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.MsOffset = s
					log.Debugf("MsOffset %f\n", n.MsOffset)
				}
			}

			re = regexp.MustCompile(`Avg (\S+) `)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.AvgRows = s
					log.Debugf("AvgRows %f\n", n.AvgRows)
				}
			}

			re = regexp.MustCompile(` x (\d+) workers`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseInt(m[1], 10, 64); err == nil {
					n.Workers = s
					log.Debugf("Workers %d\n", n.Workers)
				}
			}

			re = regexp.MustCompile(`of (\d+) scans`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseInt(m[1], 10, 64); err == nil {
					n.Scans = s
					log.Debugf("Scans %d\n", n.Scans)
				}
			}

			re = regexp.MustCompile(` \((seg\d+)\) `)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				n.MaxSeg = m[1]
				log.Debugf("MaxSeg %s\n", n.MaxSeg)
			}

			re = regexp.MustCompile(`Max (\S+) rows \(`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.MaxRows = s
				}
				log.Debugf("MaxRows %f\n", n.MaxRows)

			} else {
				// Only execute this if "Max" was not found
				re = regexp.MustCompile(` (\S+) rows \(`)
				m = re.FindStringSubmatch(line)
				if len(m) == re.NumSubexp()+1 {
					if s, err := strconv.ParseFloat(m[1], 64); err == nil {
						n.ActualRows = s
					}
					log.Debugf("ActualRows %f\n", n.ActualRows)
				}
			}
		}

		// MEMORY
		re = regexp.MustCompile(`Work_mem used`)
		if re.MatchString(line) {
			re = regexp.MustCompile(`Work_mem used:\s+(\d+)K bytes avg`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.AvgMem = s
					log.Debugf("AvgMem %f\n", n.AvgMem)
				}
			}

			re = regexp.MustCompile(`\s+(\d+)K bytes max`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.MaxMem = s
					log.Debugf("MaxMem %f\n", n.MaxMem)
				}
			}
		}

		// SPILL
		re = regexp.MustCompile(`\((\d+) spilling,\s+(\d+) reused\)`)
		m = re.FindStringSubmatch(line)
		if len(m) == re.NumSubexp()+1 {
			n.SpillFile, _ = strconv.ParseInt(strings.TrimSpace(m[1]), 10, 64)
			n.SpillReuse, _ = strconv.ParseInt(strings.TrimSpace(m[2]), 10, 64)
			log.Debugf("SpillFile %d\n", n.SpillFile)
			log.Debugf("SpillReuse %d\n", n.SpillReuse)
		}

		// PARTITION SELECTED
		re = regexp.MustCompile(`Partitions selected:  (\d+) \(out of (\d+)\)`)
		m = re.FindStringSubmatch(line)
		if len(m) == re.NumSubexp()+1 {
			n.PartSelected, _ = strconv.ParseInt(strings.TrimSpace(m[1]), 10, 64)
			n.PartTotal, _ = strconv.ParseInt(strings.TrimSpace(m[2]), 10, 64)
			log.Debugf("PartTotal %d\n", n.PartTotal)
			log.Debugf("PartSelected %d\n", n.PartSelected)
		}

		// FILTER
		re = regexp.MustCompile(`Filter: (.*)`)
		m = re.FindStringSubmatch(line)
		if len(m) == re.NumSubexp()+1 {
			n.Filter = m[1]
			log.Debugf("Filter %s\n", n.Filter)
		}

		// #Executor memory:  4978K bytes avg, 39416K bytes max (seg2).
		// if ( $info_line =~ m/Executor memory:/ ) {
		//     $exec_mem_line .= $info_line."\n";
		// }

	}

	// From Greenplum code
	//     Show elapsed time just once if they are the same or if we don't have
	//     any valid elapsed time for first tuple.
	// So set it here to avoid having to handle it later
	if n.MsFirst == -1 {
		n.MsFirst = n.MsEnd
	}

	return nil
}

// ------------------------------------------------------------
// ->  Seq Scan on sales_1_prt_outlying_years sales  (cost=0.00..67657.90 rows=2477 width=8)
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
//
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

		// Store actual status of optimizer
		if temp[0] == "optimizer" {
			e.Optimizer = temp[1]
		}
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
	e.OptimizerStatus = temp[1]
	log.Debugf("\t%s\n", e.OptimizerStatus)
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
func (e *Explain) parseLines() error {
	log.Debugf("ParseLines\n")
	log.Debugf("Parsing %d lines\n", len(e.lines))
	e.planFinished = false

	var err error
	// Loop through lines
	for e.lineOffset = 0; e.lineOffset < len(e.lines); e.lineOffset++ {
		log.Debugf("------------------------------ LINE %d ------------------------------\n", e.lineOffset+1)
		log.Debugf("%s\n", e.lines[e.lineOffset])
		err = e.parseline(e.lines[e.lineOffset])
		if err != nil {
			return err
		}
	}

	return nil
}

// Parse each line
func (e *Explain) parseline(line string) error {
	indent := getIndent(line)

	// Ignore whitespace, "QUERY PLAN" and "-"
	if len(strings.TrimSpace(line)) == 0 || strings.Index(line, "QUERY PLAN") > -1 || line[:1] == "-" {
		log.Debugf("SKIPPING\n")

	} else if patterns["NODE"].MatchString(line) {
		// Parse a new node
		newNode := e.createNode(line)

		if len(e.Nodes) == 0 && newNode.Indent != 1 {
			return errors.New(fmt.Sprintf("Wrong indentation on first plan node... expected 1, found %d:\n%s\nRecommend running EXPLAIN again and resubmitting the plan.\nDo not manually adjust the indentation as this will lead to incorrect parsing!\n", newNode.Indent, strings.TrimRight(line, " ")))
		}

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
		// Only add if node exists
		if len(e.Nodes) > 0 {
			// Append this line to ExtraInfo on the last node
			e.Nodes[len(e.Nodes)-1].ExtraInfo = append(e.Nodes[len(e.Nodes)-1].ExtraInfo, line)
		}
	} else {
		log.Debugf("SKIPPING\n")

	}

	return nil
}

// Populate SubNodes/SubPlans arrays for each node, which results
// in a tree structre with Plans[0] being the top most object:
// Plan 0
//     TopNode
//         SubNodes[]
//             Node 0
//                 SubNodes[]
//                 SubPlans[]
//             Node 1
//                 SubNodes[]
//                 SubPlans[]
//         SubPlans[]
//             Plan 0
//                 TopNode
//                     SubNodes[]
//                         Node 0
//                             SubNodes[]
//                             SubPlans[]
//                     SubPlans[]
//
func (e *Explain) BuildTree() {
	log.Debugf("########## START BUILD TREE ##########\n")

	// Walk backwards through the Plans array and a
	log.Debugf("########## PLANS ##########\n")
	for i := len(e.Plans) - 1; i > -1; i-- {
		log.Debugf("%d %s\n", e.Plans[i].Indent, e.Plans[i].Name)

		// Loop upwards to find parent
		for p := len(e.Nodes) - 1; p > -1; p-- {
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
	for i := len(e.Nodes) - 1; i > -1; i-- {
		log.Debugf("%d %s\n", e.Nodes[i].Indent, e.Nodes[i].Operator)

		foundParent := false

		// Loop upwards to find parent

		// First check for parent plans
		for p := len(e.Plans) - 1; p > -1; p-- {
			log.Debugf("\t%d %s\n", e.Plans[p].Indent, e.Plans[p].Name)
			// If the parent is a SubPlan it will always be Indent-2 and Offset-1
			//  SubPlan 1
			//    ->  Limit  (cost=0.00..9.23 rows=1 width=0)
			if (e.Nodes[i].Indent-2) == e.Plans[p].Indent && (e.Nodes[i].Offset-1) == e.Plans[p].Offset {
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
		for p := i - 1; p > -1; p-- {
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

func (n *Node) CalculateSubNodeDiff() {
	msChild := 0.0
	costChild := 0.0
	for _, s := range n.SubNodes {
		//log.Debugf("\tSUBNODE%s", s.Operator)
		msChild += s.MsEnd
		costChild += s.TotalCost
	}

	for _, s := range n.SubPlans {
		//log.Debugf("\tSUBPLANNODE%s", s.TopNode.Operator)
		costChild += s.TopNode.TotalCost
	}

	n.MsNode = n.MsEnd - msChild
	n.NodeCost = n.TotalCost - costChild
	
	if n.MsNode < 0 {
		n.MsNode = 0
	}

	if n.NodeCost < 0 {
		n.NodeCost = 0
	}
}

func (n *Node) CalculatePercentage(totalCost float64, totalMs float64) {
	n.PrctCost = n.NodeCost / totalCost * 100
	n.MsPrct = n.MsNode / totalMs * 100
}

// Render node for output to console
func (n *Node) Render(indent int) {
	indent += 1
	indentString := strings.Repeat(" ", indent*indentDepth)

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

// Render plan for output to console
func (p *Plan) Render(indent int) {
	indent += 1
	indentString := strings.Repeat(" ", indent*indentDepth)

	fmt.Printf("%s%s\n", indentString, p.Name)
	p.TopNode.Render(indent)
}

// Render explain for output to console
func (e *Explain) PrintPlan() {

	fmt.Println("Plan:")
	e.Plans[0].TopNode.Render(0)

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
		if e.MemoryWanted > 0 {
			fmt.Printf("\tMemory wanted: %d\n", e.MemoryWanted)
		}
	}

	if len(e.Settings) > 0 {
		fmt.Println("Settings:")
		for _, setting := range e.Settings {
			fmt.Printf("\t%s = %s\n", setting.Name, setting.Value)
		}
	}

	if e.OptimizerStatus != "" {
		fmt.Println("Optimizer status:")
		fmt.Printf("\t%s\n", e.OptimizerStatus)
	}

	if e.Runtime > 0 {
		fmt.Println("Total runtime:")
		fmt.Printf("\t%.0f ms\n", e.Runtime)
	}

}

// Initialize logger
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

// Main init function
func (e *Explain) InitPlan(plantext string) error {

	// Split the data in to lines
	e.lines = strings.Split(string(plantext), "\n")

	// Parse lines in to node objects
	err := e.parseLines()
	if err != nil {
		return err
	}

	if len(e.Nodes) == 0 {
		return errors.New("Could not find any nodes in plan")
	}

	// Convert array of nodes to tree structure
	e.BuildTree()

	// Parse all nodes first so they are fully populated
	for _, n := range e.Nodes {
		// Parse ExtraInfo
		err := parseNodeExtraInfo(n)
		if err != nil {
			return err
		}
	}

	// If first node is an INSERT node then it will not have any startup or total cost
	// template1=# explain insert INTO tbl1 select * from tbl1 ;
	//     Insert (slice0; segments: 4)  (rows=13200 width=32)
	//       ->  Seq Scan on tbl1  (cost=0.00..628.00 rows=13200 width=32)
	// So copy stats from the first child to make calculations work as expected
	if e.Nodes[0].TotalCost == 0 {
		if len(e.Nodes) >= 2 {
			e.Nodes[0].TotalCost = e.Nodes[1].TotalCost
			e.Nodes[0].StartupCost = e.Nodes[1].StartupCost
			e.Nodes[0].MsEnd = e.Nodes[1].MsEnd
			e.Nodes[0].MsOffset = e.Nodes[1].MsOffset
			e.Nodes[0].IsAnalyzed = e.Nodes[1].IsAnalyzed
		}
	}

	// Loop again to perform checks
	for _, n := range e.Nodes {
		n.CalculateSubNodeDiff()

		// Pass in Cost + Time of top node as it should be equal to total
		n.CalculatePercentage(e.Nodes[0].TotalCost, e.Nodes[0].MsEnd)

		// Run Node checks
		for _, c := range NODECHECKS {
			c.Exec(n)
		}
	}

	// Run Explain checks
	for _, c := range EXPLAINCHECKS {
		c.Exec(e)
	}

	return nil
}

// Init from stdin (useful for psql -f myquery.sql > planchecker)
// planchecker will handle reading from stdin
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

// Init from string
func (e *Explain) InitFromString(plantext string, debug bool) error {
	e.InitLogger(debug)

	log.Debugf("InitFromString\n")

	err := e.InitPlan(plantext)
	if err != nil {
		return err
	}

	return nil
}

// Init from file
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
