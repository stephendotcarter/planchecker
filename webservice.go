package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/pivotal-gss/planchecker/plan"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
	"math/rand"
	"time"
	"regexp"
)

// Database record
type PlanRecord struct {
	Id        int
	Ref       string
	Plantext  string
	CreatedAt string
}

var (
	// How many spaces the sub nodes should be indented
	indentDepth = 4

	// Used for random string generation
	letterRunes = []rune("1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	// Database constring
	dbconnstring string

	// Display this when running on dev instance
	testPlan = `

																							 QUERY PLAN                                                                                              
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Gather Motion 64:1  (slice12; segments: 64)  (cost=665851.67..41163590.78 rows=142 width=1073)
   ->  Nested Loop  (cost=665851.67..41163590.78 rows=3 width=1073)
		 ->  Hash Join  (cost=665577.99..794823.68 rows=1 width=1073)
			   Hash Cond: pa.org_id = hou.organization_id
			   ->  Redistribute Motion 64:64  (slice7; segments: 64)  (cost=665374.26..794619.62 rows=1 width=582)
					 Hash Key: pa.org_id
					 ->  Hash Join  (cost=665374.26..794619.18 rows=1 width=582)
						   Hash Cond: pat.carrying_out_organization_id = haou.warehouse_id
						   ->  Redistribute Motion 64:64  (slice6; segments: 64)  (cost=665170.54..794415.12 rows=1 width=91)
								 Hash Key: pat.carrying_out_organization_id
								 ->  Hash Join  (cost=665170.54..794414.68 rows=1 width=91)
									   Hash Cond: pdr.gl_date = ddim.date_value
									   ->  Redistribute Motion 64:64  (slice4; segments: 64)  (cost=664705.04..793948.84 rows=1 width=67)
											 Hash Key: pdr.gl_date
											 ->  Hash Join  (cost=664705.04..793948.40 rows=1 width=67)
												   Hash Cond: pat.project_id = pei.project_id AND pat.task_id = pei.task_id
												   ->  Seq Scan on pa_tasks pat  (cost=0.00..104639.42 rows=76887 width=23)
												   ->  Hash  (cost=663813.65..663813.65 rows=929 width=84)
														 ->  Redistribute Motion 64:64  (slice3; segments: 64)  (cost=13124.68..663813.65 rows=929 width=84)
															   Hash Key: pei.task_id
															   ->  Hash Join  (cost=13124.68..662625.12 rows=929 width=84)
																	 Hash Cond: pei.project_id = pa.project_id AND pei.attribute7::text = pdr.draft_revenue_num::text
																	 ->  Redistribute Motion 64:64  (slice2; segments: 64)  (cost=0.00..584786.26 rows=199950 width=42)
																		   Hash Key: pei.project_id
																		   ->  Seq Scan on pa_expenditure_items_all pei  (cost=0.00..328851.42 rows=199950 width=42)
																	 ->  Hash  (cost=10432.66..10432.66 rows=2805 width=51)
																		   ->  Hash Join  (cost=3327.55..10432.66 rows=2805 width=51)
																				 Hash Cond: pdr.project_id = pa.project_id
																				 ->  Seq Scan on pa_draft_revenues_all pdr  (cost=0.00..4413.09 rows=2805 width=23)
																					   Filter: transfer_status_code::text = 'A'::text
																				 ->  Hash  (cost=2391.69..2391.69 rows=1170 width=28)
																					   ->  Seq Scan on pa_projects_all pa  (cost=0.00..2391.69 rows=1170 width=28)
									   ->  Hash  (cost=457.50..457.50 rows=10 width=40)
											 ->  Redistribute Motion 1:64  (slice5)  (cost=0.00..457.50 rows=640 width=40)
												   Hash Key: ddim.date_value
												   ->  Function Scan on generate_series t  (cost=0.00..41.50 rows=640 width=8)
														 Filter: ts >= '2016-04-08 08:58:00.482988-04'::timestamp with time zone AND ts <= '2016-04-08 08:58:00.482988-04'::timestamp with time zone
						   ->  Hash  (cost=197.90..197.90 rows=8 width=514)
								 ->  Hash Join  (cost=74.48..197.90 rows=8 width=25)
									   Hash Cond: haotl.organization_id = hao.organization_id
									   ->  Seq Scan on hr_all_organization_units_tl haotl  (cost=0.00..116.43 rows=8 width=25)
											 Filter: language::text = 'US'::text
									   ->  Hash  (cost=68.66..68.66 rows=8 width=6)
											 ->  Seq Scan on hr_all_organization_units hao  (cost=0.00..68.66 rows=8 width=6)
			   ->  Hash  (cost=197.90..197.90 rows=8 width=514)
					 ->  Hash Join  (cost=74.48..197.90 rows=8 width=25)
						   Hash Cond: otl.organization_id = o.organization_id
						   ->  Seq Scan on hr_all_organization_units_tl otl  (cost=0.00..116.43 rows=8 width=25)
								 Filter: language::text = 'US'::text
						   ->  Hash  (cost=68.66..68.66 rows=8 width=6)
								 ->  Seq Scan on hr_all_organization_units o  (cost=0.00..68.66 rows=8 width=6)
		 ->  Materialize  (cost=273.69..273.70 rows=7 width=0)
			   ->  Broadcast Motion 1:64  (slice11; segments: 1)  (cost=273.67..273.69 rows=1 width=0)
					 ->  Limit  (cost=273.67..273.67 rows=1 width=0)
						   ->  Gather Motion 64:1  (slice10; segments: 64)  (cost=273.67..273.69 rows=1 width=0)
								 ->  Limit  (cost=273.67..273.67 rows=1 width=0)
									   ->  Hash Join  (cost=273.67..544.46 rows=1 width=0)
											 Hash Cond: psvlr.segment_value_lookup::text = ou.organization_name::text
											 ->  Redistribute Motion 64:64  (slice8; segments: 64)  (cost=65.16..335.68 rows=2 width=516)
												   Hash Key: psvlr.segment_value_lookup::text
												   ->  Hash Join  (cost=65.16..334.05 rows=2 width=97)
														 Hash Cond: l.segment_value_lookup_set_id = ls.segment_value_lookup_set_id
														 ->  Seq Scan on pa_segment_value_lookups l  (cost=0.00..232.69 rows=220 width=33)
														 ->  Hash  (cost=65.15..65.15 rows=1 width=71)
															   ->  Seq Scan on pa_segment_value_lookup_sets ls  (cost=0.00..65.15 rows=1 width=71)
																	 Filter: segment_value_lookup_set_name::text = 'SERVICES OPERATING UNITS'::text
											 ->  Hash  (cost=208.46..208.46 rows=1 width=498)
												   ->  Redistribute Motion 64:64  (slice9; segments: 64)  (cost=0.00..208.46 rows=1 width=498)
														 Hash Key: ou.organization_name::text
														 ->  Nested Loop  (cost=0.00..208.39 rows=1 width=25)
															   ->  Seq Scan on hr_all_organization_units o  (cost=0.00..70.99 rows=1 width=6)
																	 Filter: organization_id = $3 AND $3 = organization_id
															   ->  Seq Scan on hr_all_organization_units_tl otl  (cost=0.00..137.40 rows=1 width=25)
																	 Filter: language::text = 'US'::text AND $3 = organization_id AND organization_id = $3
		 SubPlan 1
		   ->  Result  (cost=285790.96..285791.24 rows=1 width=15)
				 Filter: conv.conversion_date = date_trunc('DAY'::text, $0) AND conv.to_currency::text = $1::text AND conv.conversion_type::text = $2::text
				 ->  Materialize  (cost=285790.96..285791.24 rows=1 width=15)
					   ->  Broadcast Motion 64:64  (slice1; segments: 64)  (cost=0.00..285790.93 rows=1 width=15)
							 ->  Seq Scan on gl_daily_rates conv  (cost=0.00..285790.93 rows=1 width=15)
								   Filter: from_currency::text = 'USD'::text
 Settings:  enable_groupagg=off; join_collapse_limit=20; optimizer=off
 Optimizer status: legacy query optimizer
(10792 rows)

Time: 36.880 ms
			";
			`
)

// Generate random string
func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

// Load HTML from file
func LoadHtml(file string) string {
	// Load HTML from file
	filedata, _ := ioutil.ReadFile(file)

	// Convert to string and return
	return string(filedata)
}

// Close database connection
func CloseDb(dbconn *sql.DB) {
	dbconn.Close()
}

// Open database connection
func OpenDb() (*sql.DB, error) {
	var err error

	dbconn, err := sql.Open("mysql", dbconnstring)
	
	if err != nil {
		return nil, err
	}

	return dbconn, nil
}

// Retrieve plan from database using ref as key
func SelectPlan(ref string) (PlanRecord, error) {
	var planRecord PlanRecord
	var err error
	
	// Open connection to DB
	dbconn, err := OpenDb()
	if err != nil {
		return planRecord, err
	}

	// Query by ref
	rows, err := dbconn.Query("SELECT * FROM plans WHERE ref = ?", ref)
	if err != nil {
		return planRecord, errors.New("Database query failed")
	}

	// Retireve the row
	count := 0
	for rows.Next() {
		err = rows.Scan(&planRecord.Id, &planRecord.Ref, &planRecord.Plantext, &planRecord.CreatedAt)
		if err != nil {
			return planRecord, errors.New("Retrieving row failed")
		}
		count++
	}

	// Close connection to DB
	CloseDb(dbconn)

	if count != 1 {
		return planRecord, errors.New(fmt.Sprintf("Expected 1 record. Found %d", count))
	}

	return planRecord, nil
}

// Insert new plan in to database and return database record
func InsertPlan(planText string) (PlanRecord, error) {
	var planRecord PlanRecord
	var err error

	// Open connection to DB
	dbconn, err := OpenDb()
	if err != nil {
		return planRecord, err
	}

	// Populate data
	// id and created_at will be populated inside database
	planRecord.Ref = RandStringRunes(8)
	planRecord.Plantext = planText

	// Prepare the statement
	stmt, err := dbconn.Prepare("INSERT plans SET ref=?,plantext=?")
	if err != nil {
		return planRecord, err
	}

	// Insert the record
	_, err = stmt.Exec(planRecord.Ref, planRecord.Plantext)
	if err != nil {
		return planRecord, err
	}

	// Close connection to DB
	CloseDb(dbconn)

	return planRecord, nil
}

func GenerateChecklistHtml() string {
	checks := ""
	checks += "<table class=\"table table-bordered table-condensed table-striped\">\n"
	checks += "<tr><th class=\"text-left\">Description</th><th class=\"text-left\">Optimizer</th><th class=\"text-left\">Added</th></tr>"
	for _, c := range plan.NODECHECKS {
		scope := ""
		for _, s := range c.Scope {
		  scope += fmt.Sprintf(" <span class=\"label optimizer-%[1]s\">%[1]s</span> ", s)
		}
		checks += fmt.Sprintf("<tr><td>%s</td><td class=\"nowrap\">%s</td><td class=\"nowrap\">%s</td></tr>", c.Description, scope, c.CreatedAt)
	}
	for _, c := range plan.EXPLAINCHECKS {
		scope := ""
		for _, s := range c.Scope {
		  scope += fmt.Sprintf(" <span class=\"label optimizer-%[1]s\">%[1]s</span> ", s)
		}
		checks += fmt.Sprintf("<tr><td>%s</td><td class=\"nowrap\">%s</td><td class=\"nowrap\">%s</td></tr>", c.Description, scope, c.CreatedAt)
	}
	checks += "</table>\n"
	return checks
}


func IndexHandler(w http.ResponseWriter, r *http.Request) {
	// Load HTML
	pageHtml := LoadHtml("templates/index.html")

	checklistHtml := GenerateChecklistHtml()

	// If we're not running in dev or local set testPlan to empty string
	re := regexp.MustCompile(`-dev`)
	fmt.Println(r.Host)
	if re.MatchString(r.Host) == false && r.Host != "localhost:8080" {
		testPlan = ""
	}

	pageHtml = fmt.Sprintf(pageHtml, testPlan, checklistHtml)

	// Print the response
	fmt.Fprintf(w, pageHtml)
}

func PlanRefHandler(w http.ResponseWriter, r *http.Request) {
	// Read plan ID
	vars := mux.Vars(r)
	planRef := vars["planRef"]

	// Print the repsonse
	planRecord, err := SelectPlan(planRef)
	if err != nil {
		fmt.Fprintf(w, "Error loading plan from database:\n--\n%s", err)
		return
	}

	GenerateExplain(w, r, planRecord, false)
}

func PlanPostHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var planText string
	var planRecord PlanRecord
	var store bool

	// Attempt to read the uploaded file
	r.ParseMultipartForm(32 << 20)
	file, _, err := r.FormFile("uploadfile")

	if err == nil {
		// If not error then try to read from file
		defer file.Close()
		buf := new(bytes.Buffer)
		n, err := buf.ReadFrom(file)
		if err != nil {
			fmt.Fprintf(w, "Error reading from file upload: %s", err)
			return
		}
		fmt.Printf("Read %d bytes from file upload", n)
		planText = buf.String()

	} else {
		// Else get the plan from POST textarea
		planText = r.FormValue("plantext")
	}

	// Check if user wants to remember the plan
	storeCheckbox := r.FormValue("store")
	if storeCheckbox == "on" {
		store = true
	} else {
		store = false
	}

	planRecord.Plantext = planText

	GenerateExplain(w, r, planRecord, store)
}

func GenerateExplain(w http.ResponseWriter, r *http.Request, planRecord PlanRecord, store bool) {

	// Create new explain object
	var explain plan.Explain
	var saveNotif string

	// Init the explain from string
	err := explain.InitFromString(planRecord.Plantext, true)
	if err != nil {
		fmt.Fprintf(w, "<!DOCTYPE html><pre>Oops... we had a problem parsing the plan:\n--\n%s\n\n<a href=\"/\">Back</a></pre>", err)
		return
	}

	// Save the plan if parsing was successful and the plan is not already saved
	if planRecord.Ref == "" && store == true {
		planRecord, err = InsertPlan(planRecord.Plantext)
		if err != nil {
			fmt.Fprintf(w, "<!DOCTYPE html><pre>Oops... we had a problem saving the plan:\n--\n%s\n\n<a href=\"/\">Back</a></pre>", err)
			return
		}
		
		// Generate full plan URL
		refUrl := fmt.Sprintf("http://%s/plan/%s", r.Host, planRecord.Ref)

		// Prompt user to save the URL
		saveNotif = fmt.Sprintf("<div class=\"alert alert-info\" style=\"margin-top:20px;\" role=\"alert\">Bookmark this URL if you want to access the results again: <strong>%s</strong></div>", refUrl)
	}

	// Generate the plan HTML
	//planHtml := explain.PrintPlanHtml()
	planHtml := RenderExplainHtml(&explain)

	// Load HTML page
	pageHtml := LoadHtml("templates/plan.html")

	// Render with the plan HTML
	fmt.Fprintf(w, pageHtml, planHtml, planRecord.Ref, saveNotif)
}

// Render node for output to HTML
func RenderNodeHtml(n *plan.Node, indent int) string {
	indent += 1
	//indentString := strings.Repeat(" ", indent * indentDepth)
	indentPixels := indent * indentDepth * 10

	HTML := fmt.Sprintf("<tr><td style=\"padding-left:%dpx\">", indentPixels)

	if n.Slice > -1 {
		HTML += fmt.Sprintf("   <span class=\"label label-success\">Slice %d</span>\n",
			n.Slice)
	}
	HTML += fmt.Sprintf("<strong>-> %s (cost=%.2f..%.2f rows=%d width=%d)</strong>\n",
		//HTML += fmt.Sprintf("%s<strong>-> %s</strong>\n",
		n.Operator,
		n.StartupCost,
		n.TotalCost,
		n.Rows,
		n.Width)

	for _, e := range n.ExtraInfo[1:] {
		HTML += fmt.Sprintf("   %s\n", strings.Trim(e, " "))
	}

	for _, w := range n.Warnings {
		HTML += fmt.Sprintf("   <span class=\"label label-danger\">WARNING: %s | %s</span>\n", w.Cause, w.Resolution)
	}

	HTML += "</td>"

	HTML += fmt.Sprintf(
		"<td class=\"text-right\">%s</td>"+
			"<td class=\"text-right\">%s</td>"+
			"<td class=\"text-right\">%.0f</td>"+
			"<td class=\"text-right\">%.0f</td>"+
			"<td class=\"text-right\">%.0f%%</td>"+
			"<td class=\"text-right\">%.0f</td>"+
			"<td class=\"text-right\">%d</td>\n",
		n.Object,
		n.ObjectType,
		n.StartupCost,
		n.NodeCost,
		n.PrctCost,
		n.TotalCost,
		n.Rows)

	if n.IsAnalyzed == true {
		if n.ActualRows > -1 {
			HTML += fmt.Sprintf(
					"<td class=\"text-right\">%.0f</td>"+
					"<td class=\"text-right\">%s</td>"+
					"<td class=\"text-right\">%s</td>"+
					"<td class=\"text-right\">%s</td>"+
					"<td class=\"text-right\">%s</td>\n",
				n.ActualRows,
				"-",
				"-",
				n.MaxSeg,
				"-")
			HTML += fmt.Sprintf(
					"<td class=\"text-right\">%.0f</td>"+
					"<td class=\"text-right\">%.0f</td>"+
					"<td class=\"text-right\">%.0f%%</td>"+
					"<td class=\"text-right\">%.0f</td>"+
					"<td class=\"text-right\">%.0f</td>",
				n.MsFirst,
				n.MsNode,
				n.MsPrct,
				n.MsEnd,
				n.MsOffset)
		} else {
			HTML += fmt.Sprintf("<td class=\"text-right\">%s</td>"+
					"<td class=\"text-right\">%.0f</td>"+
					"<td class=\"text-right\">%.0f</td>"+
					"<td class=\"text-right\">%s</td>\n"+
					"<td class=\"text-right\">%d</td>\n",
				"-",
				n.AvgRows,
				n.MaxRows,
				n.MaxSeg,
				n.Workers)
			HTML += fmt.Sprintf(
					"<td class=\"text-right\">%.0f</td>"+
					"<td class=\"text-right\">%.0f</td>"+
					"<td class=\"text-right\">%.0f%%</td>"+
					"<td class=\"text-right\">%.0f</td>"+
					"<td class=\"text-right\">%.0f</td>",
				n.MsFirst,
				n.MsNode,
				n.MsPrct,
				n.MsEnd,
				n.MsOffset)
		}
	}

	HTML += "</tr>"

	// Render sub nodes
	for _, s := range n.SubNodes {
		HTML += RenderNodeHtml(s, indent)
	}

	for _, s := range n.SubPlans {
		HTML += RenderPlanHtml(s, indent)
	}

	return HTML
}

// Render plan for output to console
func RenderPlanHtml(p *plan.Plan, indent int) string {
	HTML := ""
	indent += 1
	//indentString := strings.Repeat(" ", indent * indentDepth)
	indentPixels := indent * indentDepth * 10

	HTML += fmt.Sprintf("<tr><td style=\"padding-left:%dpx;\"><strong>%s</strong></td></tr>", indentPixels, p.Name)
	HTML += RenderNodeHtml(p.TopNode, indent)
	return HTML
}

func RenderExplainHtml(e *plan.Explain) string {
	HTML := ""
	HTML += `<table class="table table-condensed table-striped table-bordered">`
	HTML += "<tr>"
	HTMLTH1 := "<tr>"
	HTMLTH1 = "<th></th>" +
	"<th colspan=\"2\" class=\"text-center\">Object</th>" +
	"<th colspan=\"4\" class=\"text-center\">Cost</th>" +
	"<th colspan=\"1\" class=\"text-center\">Estimated</th>"
	HTMLTH2 := "<tr>"
	HTMLTH2 += "<th>Query Plan:</th>" +
		"<th class=\"text-right\">Name</th>" +
		"<th class=\"text-right\">Type</th>" +
		"<th class=\"text-right\">Startup</th>" +
		"<th class=\"text-right\">Node</th>" +
		"<th class=\"text-right\">Prct</th>" +
		"<th class=\"text-right\">Total</th>" +
		"<th class=\"text-right\">Rows</th>"
	if e.Plans[0].TopNode.IsAnalyzed == true {
		HTMLTH1 += "<th colspan=\"5\" class=\"text-center\">Row Stats</th>"
		HTMLTH2 += "<th class=\"text-right\">Actual</th>" +
			"<th class=\"text-right\">Avg</th>" +
			"<th class=\"text-right\">Max</th>" +
			"<th class=\"text-right\">Seg</th>" +
			"<th class=\"text-right\">Workers</th>"
		HTMLTH1 += "<th colspan=\"5\" class=\"text-center\">Time Ms</th>"
		HTMLTH2 += "<th class=\"text-right\">First</th>" +
			"<th class=\"text-right\">Node</th>" +
			"<th class=\"text-right\">Prct</th>" +
			"<th class=\"text-right\">End</th>" +
			"<th class=\"text-right\">Offset</th>"
	}

	HTMLTH1 += "</tr>\n"
	HTMLTH2 += "</tr>\n"

	HTML += HTMLTH1
	HTML += HTMLTH2

	HTML += RenderNodeHtml(e.Plans[0].TopNode, 0)
	HTML += `</table>`

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
		if e.MemoryWanted > 0 {
			HTML += fmt.Sprintf("\tMemory wanted: %d\n", e.MemoryWanted)
		}
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

func main() {
	// Commence randomness
	rand.Seed(time.Now().UnixNano())

	// Read port from environment
	port := os.Getenv("PORT")
	if port == "" {
		fmt.Println("PORT env variable not set")
		os.Exit(0)
	}
	fmt.Printf("Binding to port %s\n", port)

	dbconnstring = os.Getenv("CONSTRING")
	if dbconnstring == "" {
		fmt.Println("CONSTRING env variable not set")
		os.Exit(0)
	}
	fmt.Printf("Database %s\n", dbconnstring)

	// Using gorilla/mux as it provides named URL variable parsing
	r := mux.NewRouter()

	// Add handlers for each URL
	// Basic Index page
	r.HandleFunc("/", IndexHandler)

	// Server files from /assets directory
	// This avoid loading from external sources
	s := http.StripPrefix("/assets/", http.FileServer(http.Dir("./assets/")))
	r.PathPrefix("/assets/").Handler(s)

	// Reload an already submitted plan
	r.HandleFunc("/plan/{planRef}", PlanRefHandler)

	// Receive a POST form when user submits a new plan
	r.HandleFunc("/plan/", PlanPostHandler)

	// Start listening
	http.ListenAndServe(":"+port, r)
}
