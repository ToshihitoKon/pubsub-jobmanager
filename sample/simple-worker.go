package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/ToshihitoKon/pubsub-jobmanager/worker"
)

func main() {
	ctx := context.Background()

	cfg := worker.Config{
		ProjectID:        "project-id",
		TopicName:        "topic-name",
		SubscriptionName: "sub-name",
	}

	wkr, err := worker.NewWorker(ctx, cfg)
	if err != nil {
		log.Fatal("error NewWorker: ", err)
	}
	defer wkr.Close()

	wkrFunc := func(message worker.Message) {
		var msg = &SampleMessage{}
		if err := json.Unmarshal(message.Data, msg); err != nil {
			log.Println("json.Unmershal: ", err)
			return
		}
		log.Printf("Got message: %+v", msg)
	}
	if err := wkr.Subscribe(ctx, wkrFunc); err != nil {
		log.Fatal("error worker.Subscribe: ", err)
	}
}
