package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/lovoo/goka/multierr"
	"github.com/spf13/pflag"
)

const (
	parallel = 4
)

var (
	statsBasedir = pflag.String("stats-dir", "stats", "directory to stats output files")
)

type experiment struct {
	name string
	args []string
}

func main() {
	pflag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	// cancel when after a signal from terminal
	wait := make(chan os.Signal, 1)
	statsInput := make(chan string, parallel*10)
	signal.Notify(wait, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-wait:
			log.Printf("terminating due to interrupt")
			cancel()
		}
	}()

	errg, ctx := multierr.NewErrGroup(ctx)

	experiments := make(chan *experiment, 10)
	var wgWorkers sync.WaitGroup
	for i := 0; i < parallel; i++ {
		wgWorkers.Add(1)
		errg.Go(func() error {
			defer wgWorkers.Done()
			return worker(ctx, experiments, statsInput)
		})
	}

	// wait until all workers are done and close the stats input
	go func() {
		defer close(statsInput)
		wgWorkers.Wait()
	}()

	errg.Go(func() error {
		err := os.MkdirAll(*statsBasedir, os.ModePerm)
		if err != nil {
			log.Fatalf("creating stats base dir %s: %v", *statsBasedir, err)

		}
		statsFile, err := os.Create(filepath.Join(*statsBasedir, "stats"))
		if err != nil {
			return fmt.Errorf("error opening stats file: %v", err)
		}
		defer func() {
			if err := statsFile.Close(); err != nil {
				log.Printf("error closing stats file:%v", err)
			}
		}()

		fmt.Fprintf(statsFile, "storage,numkeys,keylen,vallen,recov,commit,ops.all,ops.read,ops.write,mem\n")
		for {
			select {
			case st, ok := <-statsInput:
				if !ok {
					return nil
				}
				statsFile.WriteString(st)
				statsFile.Write([]byte("\n"))
			case <-ctx.Done():
				return nil
			}
		}
	})

	errg.Go(func() error {
		return generateExperiments(ctx, experiments)
	})

	errg.Wait()
	if err := errg.Wait().NilOrError(); err != nil {
		log.Fatalf("error running: %v", err)
	}
}

func generateExperiments(ctx context.Context, exps chan *experiment) error {
	defer close(exps)
	generateExpForStorage(ctx, exps, "leveldb",
		[]int{10000,
			50000,
			100000,
			500000,
			1000000,
			4000000,
		},
		[]int{24},
		[]int{500},
		[]int{10},
	)
	generateExpForStorage(ctx, exps, "pogreb",
		[]int{10000,
			50000,
			100000,
			500000,
			1000000,
			4000000,
		},
		[]int{24},
		[]int{500},
		[]int{10},
	)
	generateExpForStorage(ctx, exps, "sqlite",
		[]int{
			10000,
			50000,
		},
		[]int{24},
		[]int{500},
		[]int{10},
	)

	return nil
}

func generateExpForStorage(ctx context.Context, exps chan *experiment, storage string, numKeys []int, keyLengths []int, valueLengths []int, nthWrites []int) {

	for _, numKey := range numKeys {
		for _, keyLength := range keyLengths {
			for _, valueLength := range valueLengths {
				for _, nthWrite := range nthWrites {
					name := fmt.Sprintf("%s-keys_%d", storage, numKey)
					exp := &experiment{
						name: name,
						args: []string{
							"run",
							"--rm",
							"-v",
							fmt.Sprintf("--name=%s", name),
							"/home/franz/docker:/out",
							"--cpus=1", "--memory=500m",
							"--device-write-iops=/dev/dm-0:300",
							"--device-read-iops=/dev/dm-0:300",
							"--device-write-bps=/dev/dm-0:5mb",
							"--device-read-bps=/dev/dm-0:5mb",
							"stbench",
							"/stbench",
							fmt.Sprintf("--storage=%s", storage),
							fmt.Sprintf("--keys=%d", numKey),
							fmt.Sprintf("--path=/out/%s", name),
							fmt.Sprintf("--key-len=%d", keyLength),
							fmt.Sprintf("--val-len=%d", valueLength),
							fmt.Sprintf("--nth-write=%d", nthWrite),
							"--clear",
							"--no-header",
							"--duration=120",
						},
					}

					select {
					case exps <- exp:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

func worker(ctx context.Context, exps <-chan *experiment, stats chan string) error {

	log.Printf("starting worker")
	defer log.Printf("worker done")
	for {
		select {
		case ex, ok := <-exps:
			// channel closed
			if !ok {
				return nil
			}

			log.Printf("running experiment %s", ex.name)
			// do the experiment
			cmd := exec.CommandContext(ctx, "docker", ex.args...)
			var (
				stdout bytes.Buffer
				stderr bytes.Buffer
			)
			cmd.Stderr = &stderr
			cmd.Stdout = &stdout
			err := cmd.Run()
			if err != nil {
				return fmt.Errorf("Error executing cmd %s: %v (stderr=%s)", ex.name, err, stderr.String())

			}

			// pipe all the stats back to the stats-channel
			for _, stat := range strings.Split(stdout.String(), "\n") {
				select {
				case stats <- stat:
				case <-ctx.Done():
					return nil
				}
			}

		case <-ctx.Done():
			return nil
		}
	}
}
