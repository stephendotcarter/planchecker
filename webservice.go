package main

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"github.com/pivotal-gss/planchecker/plan"
)

// Database record
type PlanRecord struct {
	Id        int
	Ref       string
	Plantext  string
	CreatedAt time.Time
	Saved     bool
}

var (
	// How many spaces the sub nodes should be indented
	indentDepth = 4

	// Used for random string generation
	letterRunes = []rune("1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	// Database constring
	dbconnstring string

	// Display this when running on dev instance
	testPlan string
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

	dbconn, err := sql.Open("postgres", dbconnstring)

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

	// Query by ref and save=true
	rows, err := dbconn.Query("SELECT id, ref, plantext, created_at, saved FROM plans WHERE saved = TRUE AND ref = $1", ref)
	if err != nil {
		return planRecord, errors.New("Database query failed")
	}

	// Retireve the row
	count := 0
	for rows.Next() {
		err = rows.Scan(&planRecord.Id, &planRecord.Ref, &planRecord.Plantext, &planRecord.CreatedAt, &planRecord.Saved)
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
	stmt, err := dbconn.Prepare("INSERT INTO plans(ref,plantext) VALUES($1,$2)")
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

// Insert new plan in to database and return database record
func SavePlan(planRef string) error {
	var err error

	// Open connection to DB
	dbconn, err := OpenDb()
	if err != nil {
		return err
	}

	// Prepare the statement
	stmt, err := dbconn.Prepare("UPDATE plans SET saved = true WHERE ref = $1")
	if err != nil {
		return err
	}

	// Insert the record
	_, err = stmt.Exec(planRef)
	if err != nil {
		return err
	}

	// Close connection to DB
	CloseDb(dbconn)

	return nil
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
	var err error

	// Read plan ID
	vars := mux.Vars(r)
	planRef := vars["planRef"]

	if r.Method == "GET" {
		// Get the existing plan
		planRecord, err := SelectPlan(planRef)
		if err != nil {
			fmt.Fprintf(w, "Error loading plan from database:\n--\n%s", err)
			return
		}

		GenerateExplain(w, r, planRecord, false)
	} else if r.Method == "POST" {
		err = SavePlan(planRef)
		if err != nil {
			fmt.Fprintf(w, "{\"status\":\"failure\"}")
		} else {
			fmt.Fprintf(w, "{\"status\":\"success\",\"ref\":\"%s\"}", planRef)
		}
	}
}

func PlanPostHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var planText string
	var planRecord PlanRecord

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

	// Insert the plan
	planRecord, err = InsertPlan(planText)
	if err != nil {
		fmt.Fprintf(w, "<!DOCTYPE html><pre>Oops... we had a problem parsing the plan<a href=\"/\">Back</a></pre>")
		return
	}

	GenerateExplain(w, r, planRecord, true)
}

func GenerateExplain(w http.ResponseWriter, r *http.Request, planRecord PlanRecord, isNew bool) {

	// Create new explain object
	var explain plan.Explain

	// Init the explain from string
	err := explain.InitFromString(planRecord.Plantext, true)
	if err != nil {
		fmt.Fprintf(w, "<!DOCTYPE html><pre>Oops... we had a problem parsing the plan:\n--\n%s\n\n<a href=\"/\">Back</a></pre>", err)
		return
	}

	linkState := ""
	saveState := ""
	if isNew == true {
		linkState = "hidden"
		saveState = ""
	} else {
		linkState = ""
		saveState = "hidden"
	}

	// Generate the plan HTML
	//planHtml := explain.PrintPlanHtml()
	planHtml := RenderExplainHtml(&explain)

	// Load HTML page
	pageHtml := LoadHtml("templates/plan.html")

	// Render with the plan HTML
	fmt.Fprintf(w, pageHtml, planHtml, planRecord.Ref, planRecord.Ref, linkState, saveState)
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

	if e.OptimizerStatus != "" {
		HTML += fmt.Sprintf("<strong>Optimizer status:</strong>\n")
		HTML += fmt.Sprintf("\t%s\n", e.OptimizerStatus)
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

	// Read testdata in to memory
	testPlanFilename := "testdata/explain16.txt"
	if _, err := os.Stat(testPlanFilename); os.IsNotExist(err) {
		fmt.Printf("Could not find \"%s\"\n", testPlanFilename)
		os.Exit(0)
	}

	// Read all lines
	filedata, err := ioutil.ReadFile(testPlanFilename)
	if err != nil {
		fmt.Printf("Could not load \"%s\"\n", testPlanFilename)
		os.Exit(0)
	}

	testPlan = string(filedata)

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
