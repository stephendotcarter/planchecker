/* Package multilog implements a wrapper for log package.
   Functions like  Debug, Warn, Info, and Error are wrappers
   for fmt.Printf which redirects output to Stdout and a log file simultaneously


   USAGE:
   var (
        log multilog.Mlogger
   )

   fun main() {
        log.SetupLogger("/path/to/file.log")
        log.Info("Test info")
        log.Warn("Test warning")
        log.Error("Test error")
        log.Debug("Test debug")
        log.Fatal("Test fatal")
        log.Panic("Test panic")
   }
*/

package mlogger

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

// Collect settings for terminal highlighting
const ( 
	escape = "\x1b"
	BLUE = 36
	YELLOW = 33
	RED = 31
	RESET = 0

	infoLog = 1
	warnLog = 2
	errorLog = 3
	fatalLog = 4
	panicLog = 5
	debugLog = 6



)

var Mlog Mlogger // global logger 

var (

	prefixLog = map[int]string{
					1: "INFO:", 
					2: "WARN:",
					3: "ERROR:", 
					4: "FATAL:",
					5: "PANIC:",
					6: "DEBUG:"}

	prefixStdout = map[int]string{
					1: "", 
					2: "WARN:",
					3: "ERROR:", 
					4: "FATAL:",
					5: "PANIC:",
					6: "DEBUG:"}

)

// A Mlogger object is a wrapper for dynamically controlling the log behavior
// log field is a pointer to log.logger struct
// debug is bool for enabling/disabling debug log messages
type Mlogger struct {
	log   *log.Logger
	debug bool
}

// Create a mlogger that only logs to STDOUT
// We use the io.Discard writer to avoid writing to a file as well

func init() {
	Mlog = Mlogger{log.New(ioutil.Discard, "", log.LstdFlags), false}
}

func NewStdoutOnlyLogger() (Mlogger, error) {
	mlog := Mlogger{log.New(ioutil.Discard, "", log.LstdFlags), false}
	return mlog, nil
}


// New creates a new Mlogger and passes and returns a reference to it.
// the output writer is set to the file argument passed in
func New(filename string) (Mlogger, error) {
	mlog := Mlogger{}
	logF, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		//fmt.Printf("ERROR: unable to open log file %s: %s", filename, err)
		return mlog, err
	}
	newLog := log.New(logF, "", log.LstdFlags)
	mlog = Mlogger{newLog, false}

	// Check if the permission are already 0777
	info,_ := os.Stat(filename)
	if info.Mode() != 0777 {
		// because os.OpenFile filemode will go through the OS umask settings 0777 & 0222 = 755 permissions
		// we have to force chmod of the log file to 777 so we don't have permission issues on append
		err = logF.Chmod(0777)
		if err != nil {
			fmt.Printf("Warning Failed set permission on logfile: %s", err)
		}
	}

	return mlog, nil
}

// EnableDebug sets Mlogger.debug to true enabling debug log messages
func (mlog *Mlogger) EnableDebug() {
	mlog.debug = true
}

// Sets the terminal color
func setTermColor(color int) {
	fmt.Fprintf(os.Stdout, "%s[%dm", escape, color)
}

func (mlog *Mlogger) writeOut(level int, color int, s string, v ...interface{}) {
	
	ls := prefixLog[level] + s
	ss := prefixStdout[level] + s

	if color != RESET {
		setTermColor(color)
		fmt.Printf(ss, v...)
		setTermColor(RESET)
	} else {
		fmt.Printf(ss, v...)
	}

	switch level{
	case infoLog, warnLog, errorLog, debugLog:
		mlog.log.Printf(ls, v...)
	case fatalLog:
		mlog.log.Fatalf(ls, v...)
	case panicLog:
		mlog.log.Printf(ls, v...)
		panic(fmt.Sprintf(s, v...))
	}	
}

func (mlog *Mlogger) Infof(s string, v ...interface{}) {
	mlog.writeOut(infoLog, RESET, s, v...)
}
func (mlog *Mlogger) Warnf(s string, v ...interface{}) {
	mlog.writeOut(warnLog, YELLOW, s, v...)
}
func (mlog *Mlogger) Errorf(s string, v ...interface{}) {
	mlog.writeOut(errorLog, RED, s, v...)
}
func (mlog *Mlogger) Fatalf(s string, v ...interface{}) {
	mlog.writeOut(fatalLog, RED, s, v...)
}
func (mlog *Mlogger) Panicf(s string, v ...interface{}) {
	mlog.writeOut(panicLog, RED, s, v...)
}

func (mlog *Mlogger) Debugf(s string, v ...interface{}) {
	if mlog.debug {
		mlog.writeOut(debugLog, BLUE, s, v...)
	}
}

func (mlog *Mlogger) Infoln(s string, v ...interface{}) {
	s = s + "\n"
	mlog.writeOut(infoLog, RESET, s, v...)
}

func (mlog *Mlogger) Warnln(s string, v ...interface{}) {
	s = s + "\n"
	mlog.writeOut(warnLog, YELLOW, s, v...)
}

func (mlog *Mlogger) Errorln(s string, v ...interface{}) {
	s = s + "\n"
	mlog.writeOut(errorLog, RED, s, v...)
}

func (mlog *Mlogger) Fatalln(s string, v ...interface{}) {
	s = s + "\n"
	mlog.writeOut(fatalLog, RED, s, v...)
}

func (mlog *Mlogger) Panicln(s string, v ...interface{}) {
	s = s + "\n"
	mlog.writeOut(panicLog, RED, s, v...)
}

func (mlog *Mlogger) Debugln(s string, v ...interface{}) {
	if mlog.debug {
		s = s + "\n"
		mlog.writeOut(debugLog, BLUE, s, v...)
	}
}



