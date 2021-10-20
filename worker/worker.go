package worker

import (
	"context"
	"io"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
)

type Config struct {
	ProjectID        string
	TopicName        string
	SubscriptionName string
}
type Message struct {
	Data []byte
}

type Client interface {
	io.Closer
	Subscribe(context.Context, func(Message)) error
}

type Worker struct {
	pubsubClient *pubsub.Client
	subscription *pubsub.Subscription
}

func NewWorker(ctx context.Context, cfg Config) (Client, error) {
	pubsubClient, err := pubsub.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, errors.Wrap(err, "error NewClient")
	}

	return &Worker{
		pubsubClient: pubsubClient,
		subscription: pubsubClient.Subscription(cfg.SubscriptionName),
	}, nil
}

func (w *Worker) Close() error { return w.pubsubClient.Close() }
func (w *Worker) Subscribe(ctx context.Context, fn func(Message)) error {
	if err := w.subscription.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		m.Ack()
		fn(Message{Data: m.Data})
	}); err != nil {
		return errors.Wrap(err, "error sub.Receive")
	}

	return nil
}
