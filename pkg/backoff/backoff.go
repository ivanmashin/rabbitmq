package backoff

import (
	"context"
	"errors"
	"log"
	"math"
	"time"
)

var ErrMaxAttemptsReached = errors.New("backoff reached max attempts")

type Backoff interface {
	Retry(ctx context.Context) error
	Reset()
}

func NewDefaultSigmoidBackoff() Backoff {
	return NewSigmoidBackoff(15*time.Second, 0.5, 15, 0)
}

func NewSigmoidBackoff(maxTimeout time.Duration, c1, c2 float64, maxAttempts int) Backoff {
	return &sigmoidBackoff{
		maxTimeout:  maxTimeout.Seconds(),
		c1:          c1,
		c2:          c2,
		maxAttempts: float64(maxAttempts),
	}
}

type sigmoidBackoff struct {
	maxTimeout  float64
	c1          float64
	c2          float64
	maxAttempts float64
	attempts    float64
}

func (b *sigmoidBackoff) Retry(ctx context.Context) error {
	backoff := 1 + b.maxTimeout/(1+math.Pow(math.E, -b.c1*(b.attempts-b.c2)))
	waitTime := time.Duration(backoff) * time.Second

	b.attempts++
	if b.maxAttempts > 0 && b.attempts >= b.maxAttempts {
		return ErrMaxAttemptsReached
	}

	log.Printf("Backoff retry in %s\n", waitTime.String())

	timer := time.NewTimer(waitTime)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (b *sigmoidBackoff) Reset() {
	b.attempts = 0
}
