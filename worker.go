package work

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"time"

	"github.com/gomodule/redigo/redis"
)

const fetchKeysPerJobType = 6

type worker struct {
	workerID      string
	poolID        string
	namespace     string
	pool          *redis.Pool
	jobTypes      map[string]*jobType
	sleepBackoffs []int64
	middleware    []*middlewareHandler
	contextType   reflect.Type

	redisFetchScript *redis.Script
	sampler          prioritySampler
	*observer

	stopChan         chan struct{}
	doneStoppingChan chan struct{}

	drainChan        chan struct{}
	doneDrainingChan chan struct{}
}

func newWorker(namespace string, poolID string, pool *redis.Pool, contextType reflect.Type, middleware []*middlewareHandler, jobTypes map[string]*jobType, sleepBackoffs []int64) *worker {
	workerID := makeIdentifier()
	ob := newObserver(namespace, pool, workerID)

	if len(sleepBackoffs) == 0 {
		sleepBackoffs = sleepBackoffsInMilliseconds
	}

	w := &worker{
		workerID:      workerID,
		poolID:        poolID,
		namespace:     namespace,
		pool:          pool,
		contextType:   contextType,
		sleepBackoffs: sleepBackoffs,

		observer: ob,

		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),

		drainChan:        make(chan struct{}),
		doneDrainingChan: make(chan struct{}),
	}

	w.updateMiddlewareAndJobTypes(middleware, jobTypes)

	return w
}

// note: can't be called while the thing is started
func (w *worker) updateMiddlewareAndJobTypes(middleware []*middlewareHandler, jobTypes map[string]*jobType) {
	w.middleware = middleware
	sampler := prioritySampler{}
	for _, jt := range jobTypes {
		sampler.add(jt.Priority,
			redisKeyJobs(w.namespace, jt.Name),
			redisKeyJobsInProgress(w.namespace, w.poolID, jt.Name),
			redisKeyJobsPaused(w.namespace, jt.Name),
			redisKeyJobsLock(w.namespace, jt.Name),
			redisKeyJobsLockInfo(w.namespace, jt.Name),
			redisKeyJobsConcurrency(w.namespace, jt.Name))
	}
	w.sampler = sampler
	w.jobTypes = jobTypes
	w.redisFetchScript = redis.NewScript(len(jobTypes)*fetchKeysPerJobType, redisLuaFetchJob)
}

func (w *worker) start() {
	go w.loop()
	go w.observer.start()
}

func (w *worker) stop() {
	w.stopChan <- struct{}{}
	<-w.doneStoppingChan
	w.observer.drain()
	w.observer.stop()
}

func (w *worker) drain() {
	w.drainChan <- struct{}{}
	<-w.doneDrainingChan
	w.observer.drain()
}

var sleepBackoffsInMilliseconds = []int64{0, 10, 100, 1000, 5000}

func (w *worker) loop() {
	var drained bool
	var consequtiveNoJobs int64

	// Begin immediately. We'll change the duration on each tick with a timer.Reset()
	timer := time.NewTimer(0)
	defer timer.Stop()

	// Error handling variables
	lastErrorTime := time.Now().Add(-24 * time.Hour) // Initialize to past time
	errorSuppressInterval := 60 * time.Second        // Log once per minute
	consecutiveFetchErrors := 0
	maxFetchBackoff := 5000 * time.Millisecond

	for {
		select {
		case <-w.stopChan:
			w.doneStoppingChan <- struct{}{}
			return
		case <-w.drainChan:
			drained = true
			timer.Reset(0)
		case <-timer.C:
			job, err := w.fetchJob()
			if err != nil {
				// Implement error suppression for connection errors
				consecutiveFetchErrors++

				// Log error only if enough time has passed since last error
				if time.Since(lastErrorTime) > errorSuppressInterval {
					logError("worker.fetch", err)
					lastErrorTime = time.Now()
				}

				// Calculate backoff based on consecutive errors
				backoff := 10 * time.Millisecond
				if consecutiveFetchErrors > 1 {
					// Exponential backoff with maximum
					backoffMs := math.Min(
						float64(10*math.Pow(2, float64(consecutiveFetchErrors-1))),
						float64(maxFetchBackoff/time.Millisecond),
					)
					backoff = time.Duration(backoffMs) * time.Millisecond
				}
				timer.Reset(backoff)
			} else if job != nil {
				// Log reconnection if applicable
				if consecutiveFetchErrors >= 2 {
					logWarn("worker.reconnected", fmt.Sprintf("Redis connection restored after %d consecutive errors", consecutiveFetchErrors))
				}

				consecutiveFetchErrors = 0
				w.processJob(job)
				consequtiveNoJobs = 0
				timer.Reset(0)
			} else {
				// No job case can also indicate Redis is working again
				if consecutiveFetchErrors >= 2 {
					logWarn("worker.reconnected", fmt.Sprintf("Redis connection restored after %d consecutive errors", consecutiveFetchErrors))
				}

				consecutiveFetchErrors = 0
				if drained {
					w.doneDrainingChan <- struct{}{}
					drained = false
				}
				consequtiveNoJobs++
				idx := consequtiveNoJobs
				if idx >= int64(len(w.sleepBackoffs)) {
					idx = int64(len(w.sleepBackoffs)) - 1
				}
				timer.Reset(time.Duration(w.sleepBackoffs[idx]) * time.Millisecond)
			}
		}
	}
}

func (w *worker) fetchJob() (*Job, error) {
	// resort queues
	// NOTE: we could optimize this to only resort every second, or something.
	w.sampler.sample()
	numKeys := len(w.sampler.samples) * fetchKeysPerJobType
	var scriptArgs = make([]interface{}, 0, numKeys+1)

	for _, s := range w.sampler.samples {
		scriptArgs = append(scriptArgs, s.redisJobs, s.redisJobsInProg, s.redisJobsPaused, s.redisJobsLock, s.redisJobsLockInfo, s.redisJobsMaxConcurrency) // KEYS[1-6 * N]
	}
	scriptArgs = append(scriptArgs, w.poolID) // ARGV[1]
	conn := w.pool.Get()
	defer conn.Close()

	values, err := redis.Values(w.redisFetchScript.Do(conn, scriptArgs...))
	if err == redis.ErrNil {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	if len(values) != 3 {
		return nil, fmt.Errorf("need 3 elements back")
	}

	rawJSON, ok := values[0].([]byte)
	if !ok {
		return nil, fmt.Errorf("response msg not bytes")
	}

	dequeuedFrom, ok := values[1].([]byte)
	if !ok {
		return nil, fmt.Errorf("response queue not bytes")
	}

	inProgQueue, ok := values[2].([]byte)
	if !ok {
		return nil, fmt.Errorf("response in prog not bytes")
	}

	job, err := newJob(rawJSON, dequeuedFrom, inProgQueue)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (w *worker) processJob(job *Job) {
	if job.Unique {
		updatedJob := w.getAndDeleteUniqueJob(job)
		// This is to support the old way of doing it, where we used the job off the queue and just deleted the unique key
		// Going forward the job on the queue will always be just a placeholder, and we will be replacing it with the
		// updated job extracted here
		if updatedJob != nil {
			job = updatedJob
		}
	}
	var runErr error
	jt := w.jobTypes[job.Name]
	if jt == nil {
		runErr = fmt.Errorf("stray job: no handler")
		logError("process_job.stray", runErr)
	} else {
		w.observeStarted(job.Name, job.ID, job.Args)
		job.observer = w.observer // for Checkin
		_, runErr = runJob(job, w.contextType, w.middleware, jt)
		w.observeDone(job.Name, job.ID, runErr)
	}

	fate := terminateOnly
	if runErr != nil {
		job.failed(runErr)
		fate = w.jobFate(jt, job)
	}
	w.removeJobFromInProgress(job, fate)
}

func (w *worker) getAndDeleteUniqueJob(job *Job) *Job {
	var uniqueKey string
	var err error

	if job.UniqueKey != "" {
		uniqueKey = job.UniqueKey
	} else { // For jobs put in queue prior to this change. In the future this can be deleted as there will always be a UniqueKey
		uniqueKey, err = redisKeyUniqueJob(w.namespace, job.Name, job.Args)
		if err != nil {
			logError("worker.delete_unique_job.key", err)
			return nil
		}
	}

	conn := w.pool.Get()
	defer conn.Close()

	rawJSON, err := redis.Bytes(conn.Do("GET", uniqueKey))
	if err != nil {
		logError("worker.delete_unique_job.get", err)
		return nil
	}

	_, err = conn.Do("DEL", uniqueKey)
	if err != nil {
		logError("worker.delete_unique_job.del", err)
		return nil
	}

	// Previous versions did not support updated arguments and just set key to 1, so in these cases we should do nothing.
	// In the future this can be deleted, as we will always be getting arguments from here
	if string(rawJSON) == "1" {
		return nil
	}

	// The job pulled off the queue was just a placeholder with no args, so replace it
	jobWithArgs, err := newJob(rawJSON, job.dequeuedFrom, job.inProgQueue)
	if err != nil {
		logError("worker.delete_unique_job.updated_job", err)
		return nil
	}

	return jobWithArgs
}

func (w *worker) removeJobFromInProgress(job *Job, fate terminateOp) {
	conn := w.pool.Get()
	defer conn.Close()

	conn.Send("MULTI")
	conn.Send("LREM", job.inProgQueue, 1, job.rawJSON)
	conn.Send("DECR", redisKeyJobsLock(w.namespace, job.Name))
	conn.Send("HINCRBY", redisKeyJobsLockInfo(w.namespace, job.Name), w.poolID, -1)
	fate(conn)
	if _, err := conn.Do("EXEC"); err != nil {
		logError("worker.remove_job_from_in_progress.lrem", err)
	}
}

type terminateOp func(conn redis.Conn)

func terminateOnly(_ redis.Conn) { return }
func terminateAndRetry(w *worker, jt *jobType, job *Job) terminateOp {
	rawJSON, err := job.serialize()
	if err != nil {
		logError("worker.terminate_and_retry.serialize", err)
		return terminateOnly
	}
	return func(conn redis.Conn) {
		conn.Send("ZADD", redisKeyRetry(w.namespace), nowEpochSeconds()+jt.calcBackoff(job), rawJSON)
	}
}
func terminateAndDead(w *worker, job *Job) terminateOp {
	rawJSON, err := job.serialize()
	if err != nil {
		logError("worker.terminate_and_dead.serialize", err)
		return terminateOnly
	}
	return func(conn redis.Conn) {
		// NOTE: sidekiq limits the # of jobs: only keep jobs for 6 months, and only keep a max # of jobs
		// The max # of jobs seems really horrible. Seems like operations should be on top of it.
		// conn.Send("ZREMRANGEBYSCORE", redisKeyDead(w.namespace), "-inf", now - keepInterval)
		// conn.Send("ZREMRANGEBYRANK", redisKeyDead(w.namespace), 0, -maxJobs)

		conn.Send("ZADD", redisKeyDead(w.namespace), nowEpochSeconds(), rawJSON)
	}
}

func (w *worker) jobFate(jt *jobType, job *Job) terminateOp {
	if jt != nil {
		failsRemaining := int64(jt.MaxFails) - job.Fails
		if failsRemaining > 0 {
			return terminateAndRetry(w, jt, job)
		}
		if jt.SkipDead {
			return terminateOnly
		}
	}
	return terminateAndDead(w, job)
}

// Default algorithm returns an fastly increasing backoff counter which grows in an unbounded fashion
func defaultBackoffCalculator(job *Job) int64 {
	fails := job.Fails
	return (fails * fails * fails * fails) + 15 + (rand.Int63n(30) * (fails + 1))
}
