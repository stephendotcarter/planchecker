package main
    
import (
    "os"
    "fmt"
    "bytes"
    "net/http"
    "io/ioutil"
    "github.com/gorilla/mux"
    "github.com/pivotal-gss/planchecker/plan"
)

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
        fmt.Fprintf(w, "%s\n", err)
    }

    // Generate the plan HTML
    planHtml := explain.PrintPlanHtml()

    // Load HTML page
    pageHtml := LoadHtml("templates/plan.html")

    // Render with the plan HTML
    fmt.Fprintf(w, pageHtml, planHtml)
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
