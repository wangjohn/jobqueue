package jobqueue

type Job interface {
	PerformWork()
}

type Worker struct {
	WorkerPool  chan chan Job
	JobChannel  chan Job
	QuitChannel chan bool
}

func (w Worker) Start() {
	go func() {
		for {
			w.WorkerPool <- w.JobChannel
			select {
			case job := <-w.JobChannel:
				job.PerformWork()
			case <-w.QuitChannel:
				return
			}
		}
	}()
}

func (w Worker) Stop() {
	go func() {
		w.QuitChannel <- true
	}()
}

type HiveType string

func NewWorkerDispatcher() *WorkerDispatcher {
	workerHives := make(map[HiveType]WorkerHive)
	dispatcher := WorkerDispatcher{
		WorkerHives: workerHives,
	}
	return &dispatcher
}

type WorkerDispatcher struct {
	WorkerHives map[HiveType]WorkerHive
}

type WorkerHive struct {
	Workers    []*Worker
	WorkerPool chan chan Job
	JobQueue   chan Job
}

func (w WorkerDispatcher) NewWorker(workerPool chan chan Job) Worker {
	return Worker{
		WorkerPool:  workerPool,
		JobChannel:  make(chan Job),
		QuitChannel: make(chan bool),
	}
}

func (w WorkerDispatcher) GetJobQueue(hiveType HiveType) chan Job {
	hive := w.WorkerHives[hiveType]
	return hive.JobQueue
}

func (w WorkerDispatcher) Dispatch(hiveType HiveType, jobQueue chan Job, numWorkers int) {
	workerPool := make(chan chan Job, numWorkers)
	workers := []*Worker{}
	for i := 0; i < numWorkers; i++ {
		worker := w.NewWorker(workerPool)
		worker.Start()
		workers = append(workers, &worker)
	}
	workerHive := WorkerHive{
		Workers:    workers,
		WorkerPool: workerPool,
		JobQueue:   jobQueue,
	}
	w.WorkerHives[hiveType] = workerHive

	go func() {
		for {
			select {
			case job := <-jobQueue:
				go func() {
					worker := <-workerPool
					worker <- job
				}()
			}
		}
	}()
}
