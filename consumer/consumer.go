package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// Worker is a function that can 'process' a slice of kafka messages.
type Worker func(context.Context, []kafka.Message) error

// OptionFunc updates a KafkaConsumer's properties. Intended only to be used within this package.
type OptionFunc func(*KafkaConsumer) error

// NewKafkaConsumer takes all the options provided and generates a KafkaConsumer. If any
// option setters return an error, the error will be forwarded on by this constructor.
func NewKafkaConsumer(opts ...OptionFunc) (*KafkaConsumer, error) {
	c := &KafkaConsumer{}

	for _, o := range opts {
		if err := o(c); err != nil {
			return nil, err
		}
	}

	// TODO: add more sanity checks to make sure required parameters have been set
	if c.messagesPerBatch < 1 {
		c.messagesPerBatch = 1
	}

	if c.logger == nil {
		c.logger = logrus.WithField("package", "KafkaConsumer")
	} else {
		c.logger = c.logger.WithField("package", "KafkaConsumer")
	}

	if c.ctx == nil {
		c.ctx = context.Background()
	}

	// initialize local vars
	c.messageChan = make(chan kafka.Message)

	return c, nil
}

// SetReader is a setter option function for the 'reader' parameter.
func SetReader(reader *kafka.Reader) OptionFunc {
	return func(c *KafkaConsumer) error {
		c.reader = reader
		return nil
	}
}

// SetWorkerFunc is a setter option function for the 'workerFunc' parameter.
func SetWorkerFunc(workerFunc Worker) OptionFunc {
	return func(c *KafkaConsumer) error {
		c.workerFunc = workerFunc
		return nil
	}
}

// SetMessagesPerBatch is a setter option function for the 'messagesPerBatch' parameter.
func SetMessagesPerBatch(messagesPerBatch int) OptionFunc {
	return func(c *KafkaConsumer) error {
		c.messagesPerBatch = messagesPerBatch
		return nil
	}
}

// SetBatchTimeout is a setter option function for the 'batchTimeout' parameter.
func SetBatchTimeout(batchTimeout time.Duration) OptionFunc {
	return func(c *KafkaConsumer) error {
		c.batchTimeout = batchTimeout
		return nil
	}
}

// SetLogger is a setter option function for the 'logger' parameter.
func SetLogger(logger logrus.FieldLogger) OptionFunc {
	return func(c *KafkaConsumer) error {
		c.logger = logger
		return nil
	}
}

// SetContext is a setter option function for the 'ctx' parameter.
func SetContext(ctx context.Context) OptionFunc {
	return func(c *KafkaConsumer) error {
		c.ctx = ctx
		return nil
	}
}

// KafkaReader defines the interface which dictates how KafkaConsumer should talk to Kafka.
type KafkaReader interface {
	CommitMessages(context.Context, ...kafka.Message) error
	FetchMessage(context.Context) (kafka.Message, error)
}

// KafkaConsumer consumes batches of messages from Kafka and passes them to
type KafkaConsumer struct {
	// Configurable parameters

	// reader is the configured kafka reader which we'll use to read from Kafka.
	reader KafkaReader
	// workerFunc is the function to call to process a batch of messages.
	workerFunc Worker
	// messagesPerBatch is the maximum number of messages to pass to an individual worker.
	// Any value less than 1 will be treated as 1.
	messagesPerBatch int
	// batchTimeout is the duration to wait after the first message is received for the batch
	// to reach its maximum capacity. If the batch does not reach maximum capacity within this
	// time, it'll be sent off at whatever its current size is (if > 0).
	batchTimeout time.Duration
	// logger is where log messages will be sent
	logger logrus.FieldLogger
	// ctx is the base context that will be used for all kafka requests
	ctx context.Context

	// Locals

	// messageChan is used to pass messages from the kafka queue to the intermediary queue
	// where the messages can be held for batching.
	messageChan chan kafka.Message
}

// Consume blocks to consume messages indefinitely. Can be stopped by cancelling the context.
func (c KafkaConsumer) Consume() {
	c.logger.Debug("consuming...")
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		c.consumeQueue()
		c.logger.Debug("exit consumeQueue")
	}()
	go func() {
		defer wg.Done()
		c.consumeKafka()
		c.logger.Debug("exit consumeKafka")
	}()
	wg.Wait()
}

// a blocking function to process the current batch
func (c KafkaConsumer) sendBatch(messages []kafka.Message) {
	c.logger.Debug("send batch begin: running worker...")
	err := c.workerFunc(c.ctx, messages)
	if err != nil {
		c.logger.WithError(err).Error()
		// TODO: retries? maybe don't continue processing messages?
	}

	c.logger.Debug("committing current batch...")
	err = c.reader.CommitMessages(c.ctx, messages...)
	if err != nil {
		c.logger.WithError(err).Error()
		// TODO: what actually happens when this fails?
	}

	c.logger.Debug("send batch end")
}

// consumeQueue consumes the intermediary queue where messages are held briefly for batching purposes
func (c KafkaConsumer) consumeQueue() {
	currentBatch := make([]kafka.Message, 0, c.messagesPerBatch)

	n := 0

	for {
		c.logger.Debug("consumeQueue loop")
		c.logger.Debug("currentBatch len: ", len(currentBatch))
		select {
		case m := <-c.messageChan:
			currentBatch = append(currentBatch, m)
			c.logger.Debugf("adding message to currentBatch (%d messages)", len(currentBatch))
			if len(currentBatch) == c.messagesPerBatch {
				if n == 0 {
					c.logger.Info("sending first batch")
				}
				n++
				c.logger.Debugf("currentBatch reached maximum size (%d messages), sending batch", len(currentBatch))
				c.sendBatch(currentBatch)
				currentBatch = currentBatch[:0]

				if n == 1000 {
					c.logger.Info("sent last batch")
					panic("stop!")
				}
			}

		case <-time.After(c.batchTimeout):
			if len(currentBatch) > 0 {
				c.logger.Debug("batchTimeout ended with no messages available, sending batch")
				c.sendBatch(currentBatch)
				currentBatch = currentBatch[:0]
			}
		case <-c.ctx.Done():
			c.logger.Debug("consumeQueue: context canceled")
			break
		}
	}
}

func (c KafkaConsumer) consumeKafka() {
	for {
		m, err := c.reader.FetchMessage(c.ctx)
		if err != nil {
			c.logger.WithError(err).Error()
			break
		}

		c.logger.Debugf("message at topic/partition/offset %v/%v/%v: %s = %s", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

		c.messageChan <- m
	}
}
