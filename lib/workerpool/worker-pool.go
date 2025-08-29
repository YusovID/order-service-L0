package workerpool

import (
	"context"
)

const MaxWorkersCount = 10

type Worker struct{}

type Pool[Data any] struct {
	pool    chan *Worker
	handler func(ctx context.Context, msg Data) error
}

func New[Data any](handler func(ctx context.Context, msg Data) error) *Pool[Data] {
	return &Pool[Data]{
		pool:    make(chan *Worker, MaxWorkersCount),
		handler: handler,
	}
}

func (p *Pool[Data]) Create() {
	for range MaxWorkersCount {
		p.pool <- &Worker{}
	}
}

func (p *Pool[Data]) Handle(ctx context.Context, data Data) error {
	w := <-p.pool

	defer func() { p.pool <- w }()

	return p.handler(ctx, data)
}

func (p *Pool[Data]) Wait() {
	for range MaxWorkersCount {
		<-p.pool
	}
}
