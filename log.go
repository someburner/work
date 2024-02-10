package work

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
)

func logError(key string, err error) {
	log.Errorf("work: %s - %s\n", key, err.Error())
}

func logPanic(panic Error) {
	stack, err := json.Marshal(panic.Stack)
	if err != nil {
		log.Errorf("runJob.panic - %s\n", panic.Error())
		log.Errorf("Marshalling panic: runJob.panic - %s\n", err.Error())
	}
	log.Errorf("runJob.panic: %s\n %s - %s \n %s - %s \n", string(stack), "error message", panic.Error(), "error class", panic.Class)
}