package jobqueue

type SimplePool struct {
	NumWorkers int
	workers    []SimpleWorker
	jobChannel chan Job
}

func NewSimplePool(numWorkers int) *SimplePool {
	return &SimplePool{
		NumWorkers: numWorkers,
		jobChannel: make(chan Job),
	}
}

func (p *SimplePool) AddJob(job Job) {
	p.jobChannel <- job
}

func (p *SimplePool) Run() {
	for i := 0; i < p.NumWorkers; i++ {
		worker := SimpleWorker{
			JobChannel:  p.jobChannel,
			QuitChannel: make(chan bool),
		}
		p.workers = append(p.workers, worker)
		go worker.Start()
	}
}

func (p *SimplePool) Stop() {
	for _, w := range p.workers {
		w.Stop()
	}
}

type SimpleWorker struct {
	JobChannel  chan Job
	QuitChannel chan bool
}

func (w SimpleWorker) Start() {
	go func() {
		for {
			select {
			case job := <-w.JobChannel:
				job.PerformWork()
			case <-w.QuitChannel:
				return
			}
		}
	}()
}

func (w SimpleWorker) Stop() {
	go func() {
		w.QuitChannel <- true
	}()
}
