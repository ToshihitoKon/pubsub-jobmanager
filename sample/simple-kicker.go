package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ToshihitoKon/pubsub-jobmanager/kicker"
)

func main() {
	ctx := context.Background()
	cfg := kicker.Config{
		ProjectId: "project-id",
		TopicName: "topic-name",
	}

	kcr, err := kicker.NewKicker(ctx, cfg)
	if err != nil {
		log.Fatal("NewKicker", err)
	}
	defer kcr.Close()

	message := SampleMessage{
		Text:   "sample kicker",
		Number: time.Now().Nanosecond(),
	}

	result, err := kcr.Kick(ctx, message)
	if err != nil {
		log.Fatal("kcr.Kick: ", err)
	}
	fmt.Println("kicked", result, message)
}
