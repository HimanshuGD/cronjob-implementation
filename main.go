package main

import (
	"container/heap"
	"context"
	"encoding/json"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const jobDir = "./jobs.d"

// Cron subset: five fields minute hour day month weekday.
// Weekday uses Go's time.Weekday: Sunday=0 … Saturday=6.
// Supports "*", "n", and "*/n" per field. Does not implement DOM/DOW OR, lists, or ranges.

const maxCronSteps = 366 * 24 * 60 // ~1 year of minute steps; prevents infinite scan

const (
	jobQueueBuffer    = 100
	defaultWorkerCount = 4
)

//
// =====================
// DATA STRUCTURES
// =====================
//

type Job struct {
	JobID           string `json:"job_id"`
	Description     string `json:"description"`
	Schedule        string `json:"schedule"`
	TimeoutSeconds  int    `json:"timeout_seconds,omitempty"`
	Task            Task   `json:"task"`
}

type Task struct {
	Type    string `json:"type"`
	Command string `json:"command"`
}

type ScheduledJob struct {
	Job       Job
	NextRun   time.Time
	IsOneTime bool
	runMu     sync.Mutex
}

type FileMeta struct {
	ModTime time.Time
	JobID   string
}

type heapEntry struct {
	jobID   string
	nextRun time.Time
	index   int
}

type jobHeap []*heapEntry

func (h jobHeap) Len() int { return len(h) }

func (h jobHeap) Less(i, j int) bool {
	return h[i].nextRun.Before(h[j].nextRun)
}

func (h jobHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

func (h *jobHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*heapEntry)
	item.index = n
	*h = append(*h, item)
}

func (h *jobHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	item.index = -1
	*h = old[0 : n-1]
	return item
}

//
// =====================
// JOB MANAGER
// =====================
//

type JobManager struct {
	jobs       map[string]*ScheduledJob
	mu         sync.Mutex
	notify     chan struct{}
	heapBuf    jobHeap
	jobQueue   chan *ScheduledJob
	workerCount int
	wg         sync.WaitGroup
}

func NewJobManager() *JobManager {
	return &JobManager{
		jobs:        make(map[string]*ScheduledJob),
		notify:      make(chan struct{}, 1),
		jobQueue:    make(chan *ScheduledJob, jobQueueBuffer),
		workerCount: defaultWorkerCount,
	}
}

func (jm *JobManager) signalNotify() {
	select {
	case jm.notify <- struct{}{}:
	default:
	}
}

func (jm *JobManager) Add(job Job) {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	if job.JobID == "" {
		log.Println("job_id is required; skipping job")
		return
	}
	if _, exists := jm.jobs[job.JobID]; exists {
		log.Printf("job %s already exists; use file update to replace", job.JobID)
		return
	}

	nextRun, isOneTime := computeNextRun(job)
	sj := &ScheduledJob{
		Job:       job,
		NextRun:   nextRun,
		IsOneTime: isOneTime,
	}
	jm.jobs[job.JobID] = sj
	log.Printf("Added job: %s, next run at %s", job.JobID, nextRun)
	jm.signalNotify()
}

func (jm *JobManager) Delete(jobID string) {
	if jobID == "" {
		return
	}
	jm.mu.Lock()
	defer jm.mu.Unlock()

	if _, ok := jm.jobs[jobID]; ok {
		delete(jm.jobs, jobID)
		log.Printf("Deleted job: %s", jobID)
		jm.signalNotify()
	}
}

// ReplaceJob removes previousJobID (if set) and the new job_id if present, then schedules job.
func (jm *JobManager) ReplaceJob(previousJobID string, job Job) {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	if job.JobID == "" {
		log.Println("job_id is required; skipping job update")
		return
	}
	if previousJobID != "" {
		delete(jm.jobs, previousJobID)
	}
	if previousJobID != job.JobID {
		delete(jm.jobs, job.JobID)
	}

	nextRun, isOneTime := computeNextRun(job)
	sj := &ScheduledJob{
		Job:       job,
		NextRun:   nextRun,
		IsOneTime: isOneTime,
	}
	jm.jobs[job.JobID] = sj
	log.Printf("Replaced job: %s, next run at %s", job.JobID, nextRun)
	jm.signalNotify()
}

func (jm *JobManager) RunScheduler(ctx context.Context) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	for {
		var toRun []*ScheduledJob
		var wait time.Duration

		jm.mu.Lock()
		h := jm.heapBuf[:0]
		for id, sj := range jm.jobs {
			heap.Push(&h, &heapEntry{jobID: id, nextRun: sj.NextRun})
		}
		now := time.Now()
		for h.Len() > 0 && !h[0].nextRun.After(now) {
			e := heap.Pop(&h).(*heapEntry)
			sj := jm.jobs[e.jobID]
			if sj == nil {
				continue
			}
			if !e.nextRun.Equal(sj.NextRun) {
				continue
			}
			toRun = append(toRun, sj)
			if sj.IsOneTime {
				delete(jm.jobs, sj.Job.JobID)
			} else {
				sj.NextRun = parseCronNext(sj.Job.Schedule, now)
				heap.Push(&h, &heapEntry{jobID: sj.Job.JobID, nextRun: sj.NextRun})
			}
		}
		if h.Len() == 0 {
			wait = 24 * time.Hour
		} else {
			wait = h[0].nextRun.Sub(now)
			if wait < 0 {
				wait = 0
			}
		}
		jm.heapBuf = h
		jm.stopAndResetTimer(timer, wait)
		jm.mu.Unlock()

		for _, sj := range toRun {
			jm.enqueueJobRun(sj)
		}

		select {
		case <-ctx.Done():
			return
		case <-jm.notify:
		case <-timer.C:
		}
	}
}

func (jm *JobManager) stopAndResetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	if d <= 0 {
		d = time.Nanosecond
	}
	t.Reset(d)
}

func (jm *JobManager) enqueueJobRun(sj *ScheduledJob) {
	select {
	case jm.jobQueue <- sj:
		// queued successfully
	default:
		// if queue is full, block to avoid dropping jobs and back pressure producers
		jm.jobQueue <- sj
	}
}

func (jm *JobManager) StartWorkers(ctx context.Context) {
	for i := 0; i < jm.workerCount; i++ {
		go jm.runWorker(ctx, i+1)
	}
}

func (jm *JobManager) runWorker(ctx context.Context, workerID int) {
	log.Printf("Worker %d started", workerID)
	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d stopping", workerID)
			return
		case sj := <-jm.jobQueue:
			if sj == nil {
				continue
			}

			if !sj.runMu.TryLock() {
				log.Printf("JobID=%s skipped (previous run still active)", sj.Job.JobID)
				continue
			}

			jm.wg.Add(1)
			func() {
				defer jm.wg.Done()
				defer sj.runMu.Unlock()
				jm.executeJob(ctx, sj)
			}()
		}
	}
}

func (jm *JobManager) WaitRuns() {
	jm.wg.Wait()
}

//
// =====================
// SCHEDULER LOGIC
// =====================
//

func computeNextRun(job Job) (time.Time, bool) {
	now := time.Now()

	if t, err := time.Parse(time.RFC3339, job.Schedule); err == nil {
		return t, true
	}

	return parseCronNext(job.Schedule, now), false
}

func parseCronNext(expr string, from time.Time) time.Time {
	parts := strings.Split(expr, " ")
	if len(parts) != 5 {
		log.Println("Invalid cron:", expr)
		return from.Add(time.Minute)
	}

	t := from.Truncate(time.Minute).Add(time.Minute)
	for advanced := 0; advanced < maxCronSteps; {
		if matchCron(parts, t) {
			return t
		}
		step := 1
		if s := cronMinuteStep(parts, t); s > 1 {
			step = s
		}
		t = t.Add(time.Duration(step) * time.Minute)
		advanced += step
	}
	log.Printf("cron next-run scan exceeded limit for %q; falling back to +1m", expr)
	return from.Add(time.Minute)
}

// cronMinuteStep returns n>1 when schedule is */n * * * * so we can skip minutes safely.
func cronMinuteStep(parts []string, t time.Time) int {
	if len(parts) != 5 || !strings.HasPrefix(parts[0], "*/") {
		return 1
	}
	if parts[1] != "*" || parts[2] != "*" || parts[3] != "*" || parts[4] != "*" {
		return 1
	}
	n := parseInt(parts[0][2:])
	if n <= 1 {
		return 1
	}
	m := t.Minute()
	if m%n == 0 {
		return 1
	}
	return n - (m % n)
}

func matchCron(parts []string, t time.Time) bool {
	return matchPart(parts[0], t.Minute()) &&
		matchPart(parts[1], t.Hour()) &&
		matchPart(parts[2], t.Day()) &&
		matchPart(parts[3], int(t.Month())) &&
		matchPart(parts[4], int(t.Weekday()))
}

func matchPart(expr string, value int) bool {
	if expr == "*" {
		return true
	}

	if strings.HasPrefix(expr, "*/") {
		n := parseInt(expr[2:])
		if n == 0 {
			return false
		}
		return value%n == 0
	}

	return value == parseInt(expr)
}

func parseInt(s string) int {
	n := 0
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			return 0
		}
		n = n*10 + int(ch-'0')
	}
	return n
}

//
// =====================
// EXECUTOR
// =====================
//

type TaskExecutor interface {
	Execute(ctx context.Context, task Task) error
}

type CommandExecutor struct{}

func (c *CommandExecutor) Execute(ctx context.Context, task Task) error {
	cmd := exec.CommandContext(ctx, "sh", "-c", task.Command)
	output, err := cmd.CombinedOutput()

	log.Printf("Command Output: %s", string(output))
	return err
}

var executors = map[string]TaskExecutor{
	"execute_command": &CommandExecutor{},
}

func (jm *JobManager) executeJob(rootCtx context.Context, sj *ScheduledJob) {
	start := time.Now()

	execImpl, ok := executors[sj.Job.Task.Type]
	if !ok {
		log.Printf("Unknown task type: %s", sj.Job.Task.Type)
		return
	}

	ctx := rootCtx
	var cancel context.CancelFunc
	if sj.Job.TimeoutSeconds > 0 {
		ctx, cancel = context.WithTimeout(rootCtx, time.Duration(sj.Job.TimeoutSeconds)*time.Second)
		defer cancel()
	}

	err := execImpl.Execute(ctx, sj.Job.Task)

	status := "SUCCESS"
	if err != nil {
		status = "FAILURE"
	}

	log.Printf("JobID=%s Time=%s Status=%s Desc=%s",
		sj.Job.JobID,
		start.Format(time.RFC3339),
		status,
		sj.Job.Description,
	)
}

//
// =====================
// FILE WATCHER (POLLING)
// =====================
//

var manager = NewJobManager()
var fileState = make(map[string]FileMeta)

func readJobFile(filePath string) (Job, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return Job{}, err
	}
	var job Job
	if err := json.Unmarshal(data, &job); err != nil {
		return Job{}, err
	}
	return job, nil
}

func loadJob(filePath string) string {
	job, err := readJobFile(filePath)
	if err != nil {
		log.Println("loadJob:", err)
		return ""
	}
	manager.Add(job)
	return job.JobID
}

func updateJob(filePath string, previousJobID string) string {
	job, err := readJobFile(filePath)
	if err != nil {
		log.Println("updateJob:", err)
		return ""
	}
	manager.ReplaceJob(previousJobID, job)
	return job.JobID
}

func deleteJobByFileMeta(oldPath string, meta FileMeta) {
	if meta.JobID != "" {
		manager.Delete(meta.JobID)
		return
	}
	base := filepath.Base(oldPath)
	id := strings.TrimSuffix(base, filepath.Ext(base))
	manager.Delete(id)
}

func scanDirectory() {
	currentFiles := make(map[string]FileMeta)

	files, err := os.ReadDir(jobDir)
	if err != nil {
		return
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}

		fullPath := filepath.Join(jobDir, file.Name())

		info, err := file.Info()
		if err != nil {
			continue
		}

		modTime := info.ModTime()
		oldMeta, exists := fileState[fullPath]

		if !exists {
			log.Println("New job:", fullPath)
			jobID := loadJob(fullPath)
			currentFiles[fullPath] = FileMeta{ModTime: modTime, JobID: jobID}
		} else if !modTime.Equal(oldMeta.ModTime) {
			log.Println("Updated job:", fullPath)
			jobID := updateJob(fullPath, oldMeta.JobID)
			currentFiles[fullPath] = FileMeta{ModTime: modTime, JobID: jobID}
		} else {
			currentFiles[fullPath] = oldMeta
		}
	}

	for oldPath, oldMeta := range fileState {
		if _, exists := currentFiles[oldPath]; !exists {
			log.Println("Deleted job:", oldPath)
			deleteJobByFileMeta(oldPath, oldMeta)
		}
	}

	fileState = currentFiles
}

func watchDirectoryPolling(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scanDirectory()
		}
	}
}

//
// =====================
// STARTUP
// =====================
//

func loadAllJobs() {
	files, err := os.ReadDir(jobDir)
	if err != nil {
		return
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		fullPath := filepath.Join(jobDir, file.Name())
		info, err := file.Info()
		if err != nil {
			continue
		}
		jobID := loadJob(fullPath)
		fileState[fullPath] = FileMeta{ModTime: info.ModTime(), JobID: jobID}
	}
}

func main() {
	log.Println("Starting scheduler...")

	os.MkdirAll(jobDir, 0o755)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	loadAllJobs()

	go manager.RunScheduler(ctx)
	manager.StartWorkers(ctx)
	go watchDirectoryPolling(ctx)

	<-ctx.Done()
	log.Println("Shutting down, waiting for in-flight jobs...")
	manager.WaitRuns()
	log.Println("Stopped.")
}
