package work

import (
	"fmt"
	"math"
	"time"

	"github.com/gomodule/redigo/redis"
)

type requeuer struct {
	namespace string
	pool      *redis.Pool

	redisRequeueScript *redis.Script
	redisRequeueArgs   []interface{}

	stopChan         chan struct{}
	doneStoppingChan chan struct{}

	drainChan        chan struct{}
	doneDrainingChan chan struct{}
}

func newRequeuer(namespace string, pool *redis.Pool, requeueKey string, jobNames []string) *requeuer {
	args := make([]interface{}, 0, len(jobNames)+2+2)
	args = append(args, requeueKey)              // KEY[1]
	args = append(args, redisKeyDead(namespace)) // KEY[2]
	for _, jobName := range jobNames {
		args = append(args, redisKeyJobs(namespace, jobName)) // KEY[3, 4, ...]
	}
	args = append(args, redisKeyJobsPrefix(namespace)) // ARGV[1]
	args = append(args, 0)                             // ARGV[2] -- NOTE: We're going to change this one on every call

	return &requeuer{
		namespace: namespace,
		pool:      pool,

		redisRequeueScript: redis.NewScript(len(jobNames)+2, redisLuaZremLpushCmd),
		redisRequeueArgs:   args,

		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),

		drainChan:        make(chan struct{}),
		doneDrainingChan: make(chan struct{}),
	}
}

func (r *requeuer) start() {
	go r.loop()
}

func (r *requeuer) stop() {
	r.stopChan <- struct{}{}
	<-r.doneStoppingChan
}

func (r *requeuer) drain() {
	r.drainChan <- struct{}{}
	<-r.doneDrainingChan
}

func (r *requeuer) loop() {
	// Create a proper ticker that can be replaced during backoff
	ticker := time.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()

	// Error handling variables
	consecutiveErrors := 0
	backoffDuration := 1000 * time.Millisecond
	maxBackoff := 60000 * time.Millisecond // 1 minute max backoff
	lastErrorTime := time.Now().Add(-24 * time.Hour)
	errorSuppressInterval := 60 * time.Second // Log only once per minute

	for {
		select {
		case <-r.stopChan:
			r.doneStoppingChan <- struct{}{}
			return
		case <-r.drainChan:
			success := true
			for success {
				if !r.process() {
					success = false
				}
			}
			r.doneDrainingChan <- struct{}{}
		case <-ticker.C:
			hadError := false
			didProcess := false

			for r.process() {
				didProcess = true
				// Reset error state on successful processing
				if consecutiveErrors >= 2 {
					logWarn("requeuer.reconnected", fmt.Sprintf("Redis connection restored after %d consecutive errors", consecutiveErrors))
				}

				if consecutiveErrors > 0 {
					consecutiveErrors = 0
					backoffDuration = 1000 * time.Millisecond
					ticker.Stop()
					ticker = time.NewTicker(backoffDuration)
				}
			}

			// Only check connection if we didn't successfully process anything
			if !didProcess {
				// Check if process() returned false due to an error
				conn := r.pool.Get()
				_, err := conn.Do("PING")
				conn.Close()

				if err != nil {
					hadError = true
					consecutiveErrors++

					// Only log errors once per minute
					if time.Since(lastErrorTime) > errorSuppressInterval {
						logError("requeuer.process", err)
						lastErrorTime = time.Now()
					}

					// Apply exponential backoff
					if consecutiveErrors > 1 {
						backoffDuration = time.Duration(math.Min(
							float64(backoffDuration*2),
							float64(maxBackoff),
						))
						ticker.Stop()
						ticker = time.NewTicker(backoffDuration)
					}
				} else if !hadError && consecutiveErrors >= 2 {
					// Log reconnection warning
					logWarn("requeuer.reconnected", fmt.Sprintf("Redis connection restored after %d consecutive errors", consecutiveErrors))

					// Reset on success
					consecutiveErrors = 0
					backoffDuration = 1000 * time.Millisecond
					ticker.Stop()
					ticker = time.NewTicker(backoffDuration)
				} else if !hadError && consecutiveErrors > 0 {
					// Reset without warning for just 1 error
					consecutiveErrors = 0
					backoffDuration = 1000 * time.Millisecond
					ticker.Stop()
					ticker = time.NewTicker(backoffDuration)
				}
			}
		}
	}
}

func (r *requeuer) process() bool {
	conn := r.pool.Get()
	defer conn.Close()

	r.redisRequeueArgs[len(r.redisRequeueArgs)-1] = nowEpochSeconds()

	res, err := redis.String(r.redisRequeueScript.Do(conn, r.redisRequeueArgs...))
	if err == redis.ErrNil {
		return false
	} else if err != nil {
		// We'll handle the error in the loop() function
		// so we don't log it here anymore
		return false
	}

	if res == "" {
		return false
	} else if res == "dead" {
		logError("requeuer.process.dead", fmt.Errorf("no job name"))
		return true
	} else if res == "ok" {
		return true
	}

	return false
}
