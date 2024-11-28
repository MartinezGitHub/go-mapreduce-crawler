package workerpool

import (
	"context"
	"sync"
)

// Accumulator is a function type used to aggregate values of type T into a result of type R.
// It must be thread-safe, as multiple goroutines will access the accumulator function concurrently.
// Each worker will produce intermediate results, which are combined with an initial or
// accumulated value.
type Accumulator[T, R any] func(current T, accum R) R

// Transformer is a function type used to transform an element of type T to another type R.
// The function is invoked concurrently by multiple workers, and therefore must be thread-safe
// to ensure data integrity when accessed across multiple goroutines.
// Each worker independently applies the transformer to its own subset of data, and although
// no shared state is expected, the transformer must handle any internal state in a thread-safe
// manner if present.
type Transformer[T, R any] func(current T) R

// Searcher is a function type for exploring data in a hierarchical manner.
// Each call to Searcher takes a parent element of type T and returns a slice of T representing
// its child elements. Since multiple goroutines may call Searcher concurrently, it must be
// thread-safe to ensure consistent results during recursive  exploration.
//
// Important considerations:
//  1. Searcher should be designed to avoid race conditions, particularly if it captures external
//     variables in closures.
//  2. The calling function must handle any state or values in closures, ensuring that
//     captured variables remain consistent throughout recursive or hierarchical search paths.
type Searcher[T any] func(parent T) []T

// Pool is the primary interface for managing worker pools, with support for three main
// operations: Transform, Accumulate, and List. Each operation takes an input channel, applies
// a transformation, accumulation, or list expansion, and returns the respective output.
type Pool[T, R any] interface {
	// Transform applies a transformer function to each item received from the input channel,
	// with results sent to the output channel. Transform operates concurrently, utilizing the
	// specified number of workers. The number of workers must be explicitly defined in the
	// configuration for this function to handle expected workloads effectively.
	// Since multiple workers may call the transformer function concurrently, it must be
	// thread-safe to prevent race conditions or unexpected results when handling shared or
	// internal state. Each worker independently applies the transformer function to its own
	// data subset.
	Transform(ctx context.Context, workers int, input <-chan T, transformer Transformer[T, R]) <-chan R

	// Accumulate applies an accumulator function to the items received from the input channel,
	// with results accumulated and sent to the output channel. The accumulator function must
	// be thread-safe, as multiple workers concurrently update the accumulated result.
	// The output channel will contain intermediate accumulated results as R
	Accumulate(ctx context.Context, workers int, input <-chan T, accumulator Accumulator[T, R]) <-chan R

	// List expands elements based on a searcher function, starting
	// from the given element. The searcher function finds child elements for each parent,
	// allowing exploration in a tree-like structure.
	// The number of workers should be configured based on the workload, ensuring each worker
	// independently processes assigned elements.
	List(ctx context.Context, workers int, start T, searcher Searcher[T])
}

type poolImpl[T, R any] struct{}

// New - Pool constructor
func New[T, R any]() *poolImpl[T, R] {
	return &poolImpl[T, R]{}
}

// accumulateGoroutine - description of Accumulate method goroutine
// Handles incoming data and accumulates it using the provided accumulator function.
func (p *poolImpl[T, R]) accumulateGoroutine(wg *sync.WaitGroup, ctx context.Context, input <-chan T, accumulator Accumulator[T, R], result chan R) {
	var accum R
	defer wg.Done()
	defer func() {
		// Ensures final accumulated result is sent unless context is done.
		select {
		case <-ctx.Done():
			// interrupted by ctx.Done chan
			return
		case result <- accum:
			// Send accumulated result if not interrupted.
		}
	}()
	for {
		select {
		case <-ctx.Done():
			// interrupted by ctx.Done chan
			return
		case t, ok := <-input:
			if !ok {
				// correct exit
				return
			}
			// accumulator
			accum = accumulator(t, accum)
		}
	}
}

// Accumulate - implementation of Accumulate method from Pool
// Spawns multiple goroutines to accumulate values from the input channel.
func (p *poolImpl[T, R]) Accumulate(
	ctx context.Context,
	workers int,
	input <-chan T,
	accumulator Accumulator[T, R],
) <-chan R {
	// Channel to output accumulated results.
	result := make(chan R)
	wg := sync.WaitGroup{}
	for i := 0; i < workers; i++ {
		// Start a goroutine for each worker.
		wg.Add(1)
		go p.accumulateGoroutine(&wg, ctx, input, accumulator, result)
	}
	go func() {
		defer close(result)
		wg.Wait()
	}()
	return result
}

// generator - method to move values of type T from children slice to result chan T
func (p *poolImpl[T, R]) generator(ctx context.Context, children []T, result chan T) {
	defer close(result)
	for i := 0; i < len(children); i++ {
		select {
		case <-ctx.Done():
			// interrupted by ctx.Done chan
			return
		case result <- children[i]:
			// Send each child to the result channel.
		}
	}
}

// listGoroutine - description of List method goroutine
// Processes parent elements to find children using the searcher function.
func (p *poolImpl[T, R]) listGoroutine(wg *sync.WaitGroup, ctx context.Context, parents <-chan T, searcher Searcher[T], children *[]T) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			// interrupted by ctx.Done chan
			return
		case t, ok := <-parents:
			if !ok {
				// correct exit
				return
			}
			// searcher
			*children = searcher(t)
		}
	}
}

// List - implementation of List method from Pool
// Iteratively explores elements based on searcher function in a tree-like structure.
func (p *poolImpl[T, R]) List(ctx context.Context, workers int, start T, searcher Searcher[T]) {
	children := []T{start}
	wg := sync.WaitGroup{}
	for {
		parents := make(chan T)
		for i := 0; i < workers; i++ {
			// Start a goroutine for each worker.
			wg.Add(1)
			go p.listGoroutine(&wg, ctx, parents, searcher, &children)
		}
		p.generator(ctx, children, parents)
		clear(children)
		wg.Wait()
		if len(children) == 0 {
			// Exit if no more children found.
			break
		}
		select {
		case <-ctx.Done():
			// interrupted by ctx.Done chan
			return
		default:
			// Continue searching if not interrupted.
		}
	}
}

// transformGoroutine - description of Transform method goroutine
func (p *poolImpl[T, R]) transformGoroutine(wg *sync.WaitGroup, ctx context.Context, input <-chan T, transformer Transformer[T, R], result chan R) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			// interrupted by ctx.Done chan
			return
		case t, ok := <-input:
			if !ok {
				// correct exit
				return
			}
			// transformer
			select {
			case <-ctx.Done():
				// interrupted by ctx.Done chan
				return
			case result <- transformer(t):
				// write to result chan
			}
		}
	}
}

// Transform - implementation of Transform method from Pool
// Spawns multiple goroutines to transform values from the input channel.
func (p *poolImpl[T, R]) Transform(
	ctx context.Context,
	workers int,
	input <-chan T,
	transformer Transformer[T, R],
) <-chan R {
	result := make(chan R)
	wg := sync.WaitGroup{}
	for i := 0; i < workers; i++ {
		// Start a goroutine for each worker.
		wg.Add(1)
		go p.transformGoroutine(&wg, ctx, input, transformer, result)
	}
	go func() {
		defer close(result)
		wg.Wait()
	}()
	return result
}
