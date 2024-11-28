package crawler

import (
	"context"
	"crawler/internal/fs"
	"crawler/internal/workerpool"
	"encoding/json"
	"fmt"
	"log"
	"sync"
)

// Configuration holds the configuration for the crawler, specifying the number of workers for
// file searching, processing, and accumulating tasks. The values for SearchWorkers, FileWorkers,
// and AccumulatorWorkers are critical to efficient performance and must be defined in
// every configuration.
type Configuration struct {
	SearchWorkers      int // Number of workers responsible for searching files.
	FileWorkers        int // Number of workers for processing individual files.
	AccumulatorWorkers int // Number of workers for accumulating results.
}

// Combiner is a function type that defines how to combine two values of type R into a single
// result. Combiner is not required to be thread-safe
//
// Combiner can either:
//   - Modify one of its input arguments to include the result of the other and return it,
//     or
//   - Create a new combined result based on the inputs and return it.
//
// It is assumed that type R has a neutral element (forming a monoid)
type Combiner[R any] func(current R, accum R) R

// Crawler represents a concurrent crawler implementing a map-reduce model with multiple workers
// to manage file processing, transformation, and accumulation tasks. The crawler is designed to
// handle large sets of files efficiently, assuming that all files can fit into memory
// simultaneously.
type Crawler[T, R any] interface {
	// Collect performs the full crawling operation, coordinating with the file system
	// and worker poolTransform to process files and accumulate results. The result type R is assumed
	// to be a monoid, meaning there exists a neutral element for combination, and that
	// R supports an associative combiner operation.
	// The result of this collection process, after all reductions, is returned as type R.
	//
	// Important requirements:
	// 1. Number of workers in the Configuration is mandatory for managing workload efficiently.
	// 2. FileSystem and Accumulator must be thread-safe.
	// 3. Combiner does not need to be thread-safe.
	// 4. If an accumulator or combiner function modifies one of its arguments,
	//    it should return that modified value rather than creating a new one,
	//    or alternatively, it can create and return a new combined result.
	// 5. Context cancellation is respected across workers.
	// 6. Type T is derived by json-deserializing the file contents, and any issues in deserialization
	//    must be handled within the worker.
	// 7. The combiner function will wait for all workers to complete, ensuring no goroutine leaks
	//    occur during the process.
	Collect(
		ctx context.Context,
		fileSystem fs.FileSystem,
		root string,
		conf Configuration,
		accumulator workerpool.Accumulator[T, R],
		combiner Combiner[R],
	) (R, error)
}

type crawlerImpl[T, R any] struct{}

// New instantiates a new crawler implementation.
func New[T, R any]() *crawlerImpl[T, R] {
	return &crawlerImpl[T, R]{}
}

// combinerGoroutine - description of Collect method combine goroutine
func (c *crawlerImpl[T, R]) combinerGoroutine(wg *sync.WaitGroup, ctx context.Context, accumOutput <-chan R, finalResult *R, combiner Combiner[R]) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			// interrupted by ctx.Done chan
			return
		case accum, ok := <-accumOutput:
			if !ok {
				// correct exit
				return
			}
			// Combine the current accumulated value.
			*finalResult = combiner(*finalResult, accum)
		}
	}
}

// listGoroutine - description of Collect method searcher goroutine
func (c *crawlerImpl[T, R]) listGoroutine(
	wgList *sync.WaitGroup,
	ctx context.Context,
	conf Configuration,
	root string,
	searchOutput chan string,
	searcher workerpool.Searcher[string],
	poolTransform workerpool.Pool[string, T],
) {
	defer func() {
		// Ensure searchOutput is closed when done.
		close(searchOutput)
		wgList.Done()
	}()
	// Use the searcher to list files starting from the root.
	poolTransform.List(ctx, conf.SearchWorkers, root, searcher)
}

// searcher - method to get files from one level of filesystem
func (c *crawlerImpl[T, R]) searcher(
	once *sync.Once,
	errorToWrite *error,
	fileSystem fs.FileSystem,
	ctx context.Context,
	searchOutput chan string,
) func(path string) []string {
	return func(path string) []string {
		defer func() {
			if r := recover(); r != nil {
				// panic ReadDir
				panicCheck(r, once, errorToWrite)
			}
		}()
		// read directory
		entries, err := fileSystem.ReadDir(path)
		if err != nil {
			// error ReadDir
			(*once).Do(func() {
				writeError(err, errorToWrite)
			})
			return nil
		}

		var children []string
		for _, entry := range entries {
			childPath := fileSystem.Join(path, entry.Name())
			if entry.IsDir() {
				// append directory
				children = append(children, childPath)
			} else {
				select {
				case <-ctx.Done():
					// interrupted by cts.Done chan
					return nil
				case searchOutput <- childPath:
					// send file path to the output chan
				}
			}
		}
		return children
	}
}

// transformer - method to decode files
func (c *crawlerImpl[T, R]) transformer(
	once *sync.Once,
	errorToWrite *error,
	fileSystem fs.FileSystem,
	def T,
) func(path string) T {
	return func(path string) T {
		defer func() {
			// open file panic
			if r := recover(); r != nil {
				panicCheck(r, once, errorToWrite)
			}
		}()

		// open file
		file, er := fileSystem.Open(path)
		if er != nil {
			// open file error
			once.Do(func() {
				writeError(er, errorToWrite)
			})
			return def
		}

		defer func() {
			// error while closing file
			if err := file.Close(); err != nil {
				log.Println("Error while closing file.")
			}
		}()

		var result T
		// decode json
		if err := json.NewDecoder(file).Decode(&result); err != nil {
			// json.NewDecoder error
			once.Do(func() {
				writeError(err, errorToWrite)
			})
			return def
		}

		return result
	}
}

// Collect - implementation of Collect method from Crawler interface
func (c *crawlerImpl[T, R]) Collect(
	ctx context.Context,
	fileSystem fs.FileSystem,
	root string,
	conf Configuration,
	accumulator workerpool.Accumulator[T, R],
	combiner Combiner[R],
) (R, error) {
	var errorToWrite error
	once := sync.Once{}
	poolTransform := workerpool.New[string, T]()
	searchOutput := make(chan string)

	var defaultValue T

	// wait group for List
	wgList := sync.WaitGroup{}
	wgList.Add(1)
	// search files and place them to the searchOutput chan
	go c.listGoroutine(&wgList, ctx, conf, root, searchOutput, c.searcher(&once, &errorToWrite, fileSystem, ctx, searchOutput), poolTransform)

	// decode all files from searchOutput and place parsed data to transformOutput chan
	transformOutput := poolTransform.Transform(ctx, conf.FileWorkers, searchOutput, c.transformer(&once, &errorToWrite, fileSystem, defaultValue))

	// accumulate data
	poolAccumulate := workerpool.New[T, R]()
	accumOutput := poolAccumulate.Accumulate(ctx, conf.AccumulatorWorkers, transformOutput, accumulator)
	var finalResult R

	// wait group for Combiner
	wg := sync.WaitGroup{}
	wg.Add(1)
	// combine data to result value
	go c.combinerGoroutine(&wg, ctx, accumOutput, &finalResult, combiner)

	// wait goroutines List
	wgList.Wait()
	// wait goroutine Combiner
	wg.Wait()

	// return finalResult
	select {
	case <-ctx.Done():
		// interrupted by ctx.Done chan
		return finalResult, ctx.Err()
	default:
		// not interrupted
	}

	return finalResult, errorToWrite

}

// panicCheck - method to check if there was panic
func panicCheck(r any, once *sync.Once, errorToWrite *error) {
	var err error
	if e, ok := r.(error); ok {
		err = e
	} else {
		err = fmt.Errorf("%v", r)
	}
	once.Do(func() {
		// Write error once using given error pointer.
		writeError(err, errorToWrite)
	})
}

// writeError - method to remember error
func writeError(err error, errToWrite *error) {
	*errToWrite = err
}
