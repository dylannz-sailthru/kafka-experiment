package service

import (
	"errors"
	"sync"

	"github.com/sirupsen/logrus"
)

type Job struct {
	Topic string
	Key   []byte
	Value []byte

	// TODO: NumAttempts int ??
}

type Worker func(Job) error

type Importer struct {
	numWorkers      int
	maxItemsPerTask int
	worker          Worker

	jobChan chan Job

	logger logrus.FieldLogger

	workerWaitGroup *sync.WaitGroup
}

func NewImporter(opts ...OptionFunc) (Importer, error) {
	i := &Importer{}
	for _, f := range opts {
		err := f(i)
		if err != nil {
			return *i, err
		}
	}

	if i.worker == nil {
		return *i, errors.New("no worker set (use SetWorker)")
	}

	// init channel(s)
	i.jobChan = make(chan Job)

	// start workers
	i.workerWaitGroup = &sync.WaitGroup{}
	i.workerWaitGroup.Add(i.numWorkers)
	for n := 0; n < i.numWorkers; n++ {
		go i.startWorker(n)
	}

	return *i, nil
}

type OptionFunc func(*Importer) error

func SetNumWorkers(workers int) OptionFunc {
	return OptionFunc(func(i *Importer) error {
		i.numWorkers = workers
		return nil
	})
}

func SetMaxItemsPerTask(maxItemsPerTask int) OptionFunc {
	return OptionFunc(func(i *Importer) error {
		i.maxItemsPerTask = maxItemsPerTask
		return nil
	})
}

func SetLogger(logger logrus.FieldLogger) OptionFunc {
	return OptionFunc(func(i *Importer) error {
		i.logger = logger
		return nil
	})
}

func SetWorker(worker Worker) OptionFunc {
	return OptionFunc(func(i *Importer) error {
		i.worker = worker
		return nil
	})
}

func (i Importer) Stop() {
	close(i.jobChan)
}

func (i Importer) NewJob(j Job) {
	i.jobChan <- j
}

func (i Importer) startWorker(workerID int) {
	log := i.logger.WithField("worker_id", workerID)

	for j := range i.jobChan {
		err := i.worker(j)
		if err != nil {
			// log out and we'll just have to skip this message. TODO: retries?
			log.WithError(err).Error()
			continue
		}

		log.Debug("key: ", string(j.Key))
		log.Debug("value: ", string(j.Value))
	}
}
