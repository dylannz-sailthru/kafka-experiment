package main

import (
	"context"
	"os"
	"sync"

	"github.com/dylannz/kafka-experiment/service"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/segmentio/kafka-go"
)

const (
	serviceName = "kafka-experiment"
)

type Worker struct {
	DB *mongo.Client

	Collections map[string]*mongo.Collection
}

func (w Worker) Do(j service.Job) error {
	// we're only interested in the value at the moment
	tableName := j.Topic

	collection, ok := w.Collections[tableName]
	if !ok {
		// silently ignore, we're not set up to process this topic
		return nil
	}

	// convert from JSON to BSON
	doc := bson.D{}
	err := bson.UnmarshalExtJSON(j.Value, true, &doc)
	if err != nil {
		return errors.Wrap(err, "unmarshal JSON")
	}

	ctx := context.Background() // TODO: pass this in...
	/*res*/ _, err = collection.InsertMany(ctx, []interface{}{doc}) // TODO: parse multiple documents
	if err != nil {
		return err
	}

	// res.InsertedIDs

	return nil
}

func setLogLevel() error {
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}
	parsedLevel, err := logrus.ParseLevel(logLevel)
	if err != nil {
		return err
	}

	logrus.SetLevel(parsedLevel)
	return nil
}

func main() {

	log := logrus.WithField("service", serviceName)

	err := godotenv.Load()
	if err != nil {
		log.Fatal(errors.Wrap(err, "load .env file"))
	}

	err = setLogLevel()
	if err != nil {
		log.Fatal(errors.Wrap(err, "load/parse log level"))
	}

	log.Debug("connecting to mongo: ", os.Getenv("MONGO_URI"))
	ctx := context.Background()
	db, err := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("MONGO_URI")))
	if err != nil {
		log.Fatal(errors.Wrap(err, "connect to mongo"))
	}

	log.Debug("setting up worker/topics")
	worker := Worker{
		// insert mongo db connection here
		DB:          db,
		Collections: map[string]*mongo.Collection{},
	}

	topics := []string{"messages_delivered"}
	for _, t := range topics {
		worker.Collections[t] = db.Database(os.Getenv("MONGO_DB_NAME")).Collection(t)
	}

	log.Debug("creating/configuring importer...")
	importer, err := service.NewImporter(
		service.SetNumWorkers(5),
		//service.SetMaxItemsPerTask(), TODO
		service.SetLogger(log),
		service.SetWorker(worker.Do),
	)
	if err != nil {
		log.Fatal(errors.Wrap(err, "initialize importer"))
	}

	log.Debug("starting workers...")
	configs := []kafka.ReaderConfig{
		{
			Brokers: []string{"localhost:9092"},
			//GroupID:  "kafka-experiment",
			Topic:    "messages_delivered",
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		},
	}

	wg := sync.WaitGroup{}
	wg.Add(len(configs))
	for _, c := range configs {
		go consumeKafka(log, importer.NewJob, c)
	}
	log.Info("Ready for messages")
	wg.Wait()
	log.Info("all goroutines finished, exiting...")
}

func consumeKafka(log logrus.FieldLogger, fn func(service.Job), cfg kafka.ReaderConfig) {
	// make a new reader that consumes from topic-A
	r := kafka.NewReader(cfg)

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}

		log.Debugf("message at topic/partition/offset %v/%v/%v: %s = %s", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

		fn(service.Job{
			Topic: m.Topic,
			Key:   m.Key,
			Value: m.Value,
		})
	}

	r.Close()
}
