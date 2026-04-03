package queue

import (
	"context"
)

type Submitter struct {
	queue Queue
}

func NewSubmitter(queue Queue) *Submitter {
	return &Submitter{queue: queue}
}

func (s *Submitter) Submit(ctx context.Context, msg JobMessage) error {
	return s.queue.Submit(ctx, msg)
}
