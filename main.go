package main

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/dylannz/kafka-experiment/consumer"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/segmentio/kafka-go"

	"net/http"
	_ "net/http/pprof"
)

const (
	serviceName = "kafka-experiment"
)

type Worker struct {
	DB *mongo.Client

	Collection *mongo.Collection
}

func (w Worker) Do(ctx context.Context, messages []kafka.Message) error {
	docs := make([]interface{}, len(messages))

	for k, m := range messages {
		doc := bson.D{}
		// convert from JSON to BSON
		// TODO: handle key as well?
		err := bson.UnmarshalExtJSON(m.Value, true, &doc)
		if err != nil {
			return errors.Wrap(err, "unmarshal JSON")
		}

		docs[k] = doc
	}

	/*res*/
	_, err := w.Collection.InsertMany(ctx, docs)
	if err != nil {
		return errors.Wrap(err, "insert many into collection")
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
	go func() {
		// for pprof
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

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

	database := db.Database(os.Getenv("MONGO_DB_NAME"))

	log.Debug("starting consumers and workers...")
	configs := []kafka.ReaderConfig{
		{
			Brokers:  []string{"localhost:9092"},
			GroupID:  "kafka-experiment_6",
			Topic:    "messages_delivered",
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		},
	}

	wg := sync.WaitGroup{}
	wg.Add(len(configs))
	for _, c := range configs {
		worker := Worker{
			DB:         db,
			Collection: database.Collection(c.Topic),
		}

		c, err := consumer.NewKafkaConsumer(
			consumer.SetReader(kafka.NewReader(c)),
			consumer.SetWorkerFunc(worker.Do),
			consumer.SetMessagesPerBatch(1000),
			consumer.SetBatchTimeout(time.Second*30),
			consumer.SetContext(ctx),
			consumer.SetLogger(log),
		)
		if err != nil {
			log.WithError(err).Fatal()
		}
		go func() {
			defer wg.Done()
			c.Consume()
		}()
	}
	log.Info("Ready for messages")
	wg.Wait()
	log.Info("all goroutines finished, exiting...")
}
