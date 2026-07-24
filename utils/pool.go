package utils

import (
	"errors"
	"sync"
)

type Pool struct {
	taskQueue chan Task
	workerNum int
	wg        sync.WaitGroup
	stopOnce  sync.Once
	stopped   bool
}

type Task func()

func NewPool(workerNum int, queueSize int) *Pool {
	p := &Pool{
		taskQueue: make(chan Task, queueSize),
		workerNum: workerNum,
	}
	p.start()
	return p
}

func (p *Pool) start() {
	for i := 0; i < p.workerNum; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for task := range p.taskQueue {
		task()
	}
}

func (p *Pool) Submit(task Task) error {
	if p.stopped {
		return errors.New("pool has been stopped")
	}
	p.taskQueue <- task
	return nil
}

func (p *Pool) Shutdown() {
	p.stopOnce.Do(func() {
		p.stopped = true
		close(p.taskQueue)
		p.wg.Wait()
	})
}
