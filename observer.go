package work

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/gomodule/redigo/redis"
)

// An observer observes a single worker. Each worker has its own observer.
type observer struct {
	namespace string
	workerID  string
	pool      *redis.Pool

	// nil: worker isn't doing anything that we know of
	// not nil: the last started observation that we received on the channel.
	// if we get an checkin, we'll just update the existing observation
	currentStartedObservation *observation

	// version of the data that we wrote to redis.
	// each observation we get, we'll update version. When we flush it to redis, we'll update lastWrittenVersion.
	// This will keep us from writing to redis unless necessary
	version, lastWrittenVersion int64

	observationsChan chan *observation

	stopChan         chan struct{}
	doneStoppingChan chan struct{}

	drainChan        chan struct{}
	doneDrainingChan chan struct{}
}

type observationKind int

const (
	observationKindStarted observationKind = iota
	observationKindDone
	observationKindCheckin
)

type observation struct {
	kind observationKind

	// These fields always need to be set
	jobName string
	jobID   string

	// These need to be set when starting a job
	startedAt int64
	arguments map[string]interface{}

	// If we're done w/ the job, err will indicate the success/failure of it
	err error // nil: success. not nil: the error we got when running the job

	// If this is a checkin, set these.
	checkin   string
	checkinAt int64
}

const observerBufferSize = 1024

func newObserver(namespace string, pool *redis.Pool, workerID string) *observer {
	return &observer{
		namespace:        namespace,
		workerID:         workerID,
		pool:             pool,
		observationsChan: make(chan *observation, observerBufferSize),

		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),

		drainChan:        make(chan struct{}),
		doneDrainingChan: make(chan struct{}),
	}
}

func (o *observer) start() {
	go o.loop()
}

func (o *observer) stop() {
	o.stopChan <- struct{}{}
	<-o.doneStoppingChan
}

func (o *observer) drain() {
	o.drainChan <- struct{}{}
	<-o.doneDrainingChan
}

func (o *observer) observeStarted(jobName, jobID string, arguments map[string]interface{}) {
	o.observationsChan <- &observation{
		kind:      observationKindStarted,
		jobName:   jobName,
		jobID:     jobID,
		startedAt: nowEpochSeconds(),
		arguments: arguments,
	}
}

func (o *observer) observeDone(jobName, jobID string, err error) {
	o.observationsChan <- &observation{
		kind:    observationKindDone,
		jobName: jobName,
		jobID:   jobID,
		err:     err,
	}
}

func (o *observer) observeCheckin(jobName, jobID, checkin string) {
	o.observationsChan <- &observation{
		kind:      observationKindCheckin,
		jobName:   jobName,
		jobID:     jobID,
		checkin:   checkin,
		checkinAt: nowEpochSeconds(),
	}
}

func (o *observer) loop() {
	// Initial ticker that will be replaced during backoff
	ticker := time.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()

	// Track consecutive errors for backoff
	consecutiveErrors := 0
	backoffDuration := 1000 * time.Millisecond
	maxBackoff := 120000 * time.Millisecond          // Increase max backoff to 2 minutes
	lastErrorTime := time.Now().Add(-24 * time.Hour) // Initialize to a past time
	errorSuppressInterval := 60 * time.Second        // Only log same error once per minute

	for {
		select {
		case <-o.stopChan:
			o.doneStoppingChan <- struct{}{}
			return
		case <-o.drainChan:
		DRAIN_LOOP:
			for {
				select {
				case obv := <-o.observationsChan:
					o.process(obv)
				default:
					if err := o.writeStatus(o.currentStartedObservation); err != nil {
						// Only log errors once per minute to avoid filling logs
						if time.Since(lastErrorTime) > errorSuppressInterval {
							logError("observer.write", err)
							lastErrorTime = time.Now()
						}
					}
					o.doneDrainingChan <- struct{}{}
					break DRAIN_LOOP
				}
			}
		case <-ticker.C:
			if o.lastWrittenVersion != o.version {
				if err := o.writeStatus(o.currentStartedObservation); err != nil {
					// Only log errors once per minute to avoid filling logs
					if time.Since(lastErrorTime) > errorSuppressInterval {
						logError("observer.write", err)
						lastErrorTime = time.Now()
					}

					// Increase consecutive errors and adjust ticker for backoff
					consecutiveErrors++
					if consecutiveErrors > 1 {
						// Exponential backoff with maximum limit
						backoffDuration = time.Duration(math.Min(
							float64(backoffDuration*2),
							float64(maxBackoff),
						))

						// Replace ticker with a new one using backoff duration
						ticker.Stop()
						ticker = time.NewTicker(backoffDuration)
					}
				} else {
					// Reset on success and log reconnection if applicable
					if consecutiveErrors >= 2 {
						logWarn("observer.reconnected", fmt.Sprintf("Redis connection restored after %d consecutive errors", consecutiveErrors))
					}

					if consecutiveErrors > 0 {
						consecutiveErrors = 0
						backoffDuration = 1000 * time.Millisecond
						ticker.Stop()
						ticker = time.NewTicker(backoffDuration)
					}
					o.lastWrittenVersion = o.version
				}
			}
		case obv := <-o.observationsChan:
			o.process(obv)
		}
	}
}

func (o *observer) process(obv *observation) {
	if obv.kind == observationKindStarted {
		o.currentStartedObservation = obv
	} else if obv.kind == observationKindDone {
		o.currentStartedObservation = nil
	} else if obv.kind == observationKindCheckin {
		if (o.currentStartedObservation != nil) && (obv.jobID == o.currentStartedObservation.jobID) {
			o.currentStartedObservation.checkin = obv.checkin
			o.currentStartedObservation.checkinAt = obv.checkinAt
		} else {
			logError("observer.checkin_mismatch", fmt.Errorf("got checkin but mismatch on job ID or no job"))
		}
	}
	o.version++

	// If this is the version observation we got, just go ahead and write it.
	if o.version == 1 {
		if err := o.writeStatus(o.currentStartedObservation); err != nil {
			logError("observer.first_write", err)
		}
		o.lastWrittenVersion = o.version
	}
}

func (o *observer) writeStatus(obv *observation) error {
	conn := o.pool.Get()
	defer conn.Close()

	key := redisKeyWorkerObservation(o.namespace, o.workerID)

	if obv == nil {
		if _, err := conn.Do("DEL", key); err != nil {
			return err
		}
	} else {
		// hash:
		// job_name -> obv.Name
		// job_id -> obv.jobID
		// started_at -> obv.startedAt
		// args -> json.Encode(obv.arguments)
		// checkin -> obv.checkin
		// checkin_at -> obv.checkinAt

		var argsJSON []byte
		if len(obv.arguments) == 0 {
			argsJSON = []byte("")
		} else {
			var err error
			argsJSON, err = json.Marshal(obv.arguments)
			if err != nil {
				return err
			}
		}

		args := make([]interface{}, 0, 13)
		args = append(args,
			key,
			"job_name", obv.jobName,
			"job_id", obv.jobID,
			"started_at", obv.startedAt,
			"args", argsJSON,
		)

		if (obv.checkin != "") && (obv.checkinAt > 0) {
			args = append(args,
				"checkin", obv.checkin,
				"checkin_at", obv.checkinAt,
			)
		}

		conn.Send("HMSET", args...)
		conn.Send("EXPIRE", key, 60*60*24)
		if err := conn.Flush(); err != nil {
			return err
		}

	}

	return nil
}
