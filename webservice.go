package main

import (
	"bytes"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/pivotal-gss/planchecker/plan"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

var indentDepth = 4

func LoadHtml(file string) string {
	// Load HTML from file
	filedata, _ := ioutil.ReadFile(file)

	// Convert to string and return
	return string(filedata)
}

func IndexHandler(w http.ResponseWriter, r *http.Request) {
	// Load HTML
	pageHtml := LoadHtml("templates/index.html")

	// Print the response
	fmt.Fprintf(w, pageHtml)
}

/*
func PlanHandler(w http.ResponseWriter, r *http.Request) {
    // Read plan ID
    vars := mux.Vars(r)
    planId := vars["planId"]

    // Print the repsonse
    fmt.Fprintf(w, "Plan %s", planId)
}
*/

func PlanPostHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var planText string

	// Attempt to read the uploaded file
	r.ParseMultipartForm(32 << 20)
	file, _, err := r.FormFile("uploadfile")

	if err == nil {
		// If not error then try to read from file
		defer file.Close()
		buf := new(bytes.Buffer)
		n, err := buf.ReadFrom(file)
		if err != nil {
			fmt.Printf("Error reading from file upload: %s", err)
		}
		fmt.Printf("Read %d bytes from file upload", n)
		planText = buf.String()

	} else {
		// Else get the plan from POST textarea
		planText = r.FormValue("plantext")
	}

	// Create new explain object
	var explain plan.Explain

	// Init the explain from string
	err = explain.InitFromString(planText, true)
	if err != nil {
		fmt.Fprintf(w, "Oops... we had a problem parsing the plan:\n--\n%s\n", err)
		return
	}

	// Generate the plan HTML
	//planHtml := explain.PrintPlanHtml()
	planHtml := RenderExplainHtml(&explain)

	// Load HTML page
	pageHtml := LoadHtml("templates/plan.html")

	// Render with the plan HTML
	fmt.Fprintf(w, pageHtml, planHtml)
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
		HTMLTH1 += "<th colspan=\"6\" class=\"text-center\">Row Stats</th>"
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

func main() {

	port := os.Getenv("PORT")

	if port == "" {
		fmt.Println("PORT env variable not set")
		os.Exit(0)
	}

	fmt.Printf("Binding to port %s\n", port)

	// Using gorilla/mux as it provides named URL variable parsing
	r := mux.NewRouter()

	// Add handlers for each URL
	// Basic Index page
	r.HandleFunc("/", IndexHandler)

	// Reload an already submitted plan
	//r.HandleFunc("/plan/{planId}", PlanHandler)

	// Receive a POST form when user submits a new plan
	r.HandleFunc("/plan/", PlanPostHandler)

	// Start listening
	http.ListenAndServe(":"+port, r)
}
