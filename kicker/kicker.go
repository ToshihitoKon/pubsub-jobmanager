package kicker

import (
	"context"
	"encoding/json"
	"io"
	"log"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
)

type KickResult string
type Client interface {
	Kick(context.Context, interface{}) (KickResult, error)
	io.Closer
}
type Config struct {
	ProjectId string
	TopicName string
}

type Kicker struct {
	pubsubClient *pubsub.Client
	topic        *pubsub.Topic
}

func NewKicker(ctx context.Context, cfg Config) (Client, error) {
	pubsubClient, err := pubsub.NewClient(ctx, cfg.ProjectId)
	if err != nil {
		return nil, errors.Wrap(err, "error NewClient")
	}

	return &Kicker{
		pubsubClient: pubsubClient,
		topic:        pubsubClient.Topic(cfg.TopicName),
	}, nil
}

func (k *Kicker) Close() error { return k.pubsubClient.Close() }
func (k *Kicker) Kick(ctx context.Context, data interface{}) (KickResult, error) {
	bytes, err := json.Marshal(data)
	if err != nil {
		log.Fatal("json.Marshal: ", err)
	}

	result := k.topic.Publish(ctx, &pubsub.Message{
		Data: bytes,
	})
	id, err := result.Get(ctx)
	if err != nil {
		return "", errors.Wrap(err, "error: result.Get")
	}
	return KickResult(id), nil
}
