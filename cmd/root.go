package cmd

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var (
	query    string
	dsn      string
	duration int
)

func init() {
	rootCmd.Flags().StringVarP(&query, "query", "q", "", "SQL query to execute (required)")
	rootCmd.Flags().StringVarP(&dsn, "dsn", "d", "", "PostgreSQL connection string (required)")
	rootCmd.Flags().IntVarP(&duration, "duration", "t", 0, "Test duration in milliseconds (required)")
}

var rootCmd = &cobra.Command{
	Use:   "pg-rps-bench --query QUERY --dsn DSN --duration DURATION",
	Short: "Database query load tester",
	Long:  `The "pg-rps-bench" command performs a database load test by executing the provided SQL query within a specified time.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if cmd.Flags().NFlag() == 0 {
			err := cmd.Help()
			if err != nil {
				return err
			}
			return nil
		}
		if err := validateFlags(); err != nil {
			return err
		}

		runBench()
		return nil
	},
}

func Execute() error {
	return rootCmd.Execute()
}

func validateFlags() error {
	if query == "" {
		return errors.New("the --query (-q) flag is required. Please specify the SQL query to execute")
	}
	if dsn == "" {
		return errors.New("the --dsn (-d) flag is required. Please specify the PostgreSQL connection string")
	}
	if duration <= 0 {
		return errors.New("the --duration (-t) flag is required. Please specify the test duration in milliseconds (> 0)")
	}

	return nil
}

func runBench() {
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		fmt.Printf("Failed to create connection pool: %v\n", err)
		os.Exit(1)
	}

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()

	if err = pool.Ping(pingCtx); err != nil {
		fmt.Printf("Failed to connect to database: %v\n", err)
		os.Exit(2)
	}
	defer pool.Close()

	var errorsCount atomic.Int64
	errLimit := int64(10)

	timeBasedErrLimit := int64(duration * 2 / 1000)
	if timeBasedErrLimit > errLimit {
		errLimit = timeBasedErrLimit
	}

	var totalRequests atomic.Int64
	concurrency := runtime.NumCPU()
	var wg sync.WaitGroup
	wg.Add(concurrency)

	testCtx, testCancel := context.WithTimeout(context.Background(), time.Duration(duration)*time.Millisecond)
	defer testCancel()

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-testCtx.Done():
					return
				default:
					conn, err := pool.Acquire(context.Background())
					if err != nil {
						fmt.Printf("Failed to acquire connection: %v\n", err)
						continue
					}

					_, err = conn.Exec(testCtx, query)
					conn.Release()

					if err == nil {
						totalRequests.Add(1)
					} else if !pgconn.Timeout(err) && !errors.Is(err, context.DeadlineExceeded) {
						currentErrors := errorsCount.Add(1)
						if currentErrors <= errLimit {
							fmt.Printf("Query execution error (%d): %v\n", currentErrors, err)
						}
						if currentErrors == errLimit {
							fmt.Printf("Too many errors encountered (%d). Exiting...", errLimit)
							os.Exit(3)
						}
					}
				}
			}
		}()
	}

	wg.Wait()
	rps := float64(totalRequests.Load()) * 1000 / float64(duration)
	fmt.Printf("RPS: %.2f\n", rps)
}
