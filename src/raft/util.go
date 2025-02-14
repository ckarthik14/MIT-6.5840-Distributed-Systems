package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// logTopic defines the type for log topics.
type logTopic string

// Constants for different log topics.
const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dElect   logTopic = "ELEC"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

// DEBUG indicates whether debugging is enabled.
var DEBUG bool

// debugStart records the start time for debugging.
var debugStart time.Time

func init() {
	// Initialize debugging based on the RAFT_DEBUG environment variable.
	debugStart = time.Now()
	DEBUG = getVerbosity() >= 1

	// Configure log package to omit date and time since we'll add custom timestamps.
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

// getVerbosity retrieves the verbosity level from the RAFT_DEBUG environment variable.
func getVerbosity() int {
	v := os.Getenv("DEBUG")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity level: %v", v)
		}
	}
	return level
}

// DPrintf logs debug messages with server ID, topic, and formatted content when DEBUG is enabled.
func DPrintf(serverID int, topic logTopic, format string, a ...interface{}) {
	if !DEBUG || !(topic == dLog || topic == dElect) {
		return
	}

	// Calculate milliseconds since the start of debugging.
	elapsed := time.Since(debugStart).Milliseconds()

	// Construct the log prefix with elapsed time, server ID, and topic.
	prefix := fmt.Sprintf("%06d S%d %s ", elapsed, serverID, topic)

	// Format the final log message.
	logMsg := fmt.Sprintf(format, a...)
	finalLog := prefix + logMsg

	// Use log.Output to ensure correct caller information.
	log.Output(2, finalLog)
}
