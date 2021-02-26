package main

import (
	"io/ioutil"
	"log"
	"strconv"
	"strings"

	"github.com/lovoo/goka-tools/localstorage"
	"github.com/spf13/pflag"
)

var (
	baseFolder = pflag.String("base-folder", "", "base folder to check for level dbs")
	table      = pflag.String("table", "", "table to construct storage subfolder")
	partitions = pflag.Int32Slice("partitions", nil, "list of partitions to include from the base folder. Uses all if not present")

	printKeys = pflag.Bool("print-keys", false, "print keys")
	limit     = pflag.Int("limit", 100, "how many items to print")
	count     = pflag.Bool("count", false, "print count of items")
)

func main() {
	pflag.Parse()

	files, err := ioutil.ReadDir(*baseFolder)
	if err != nil {
		log.Fatalf("Error listing directory: %v", err)
	}

	var parts = make([]int32, len(*partitions))
	// copy parts
	copy(parts, *partitions)

	if len(parts) == 0 {
		for _, file := range files {
			if !file.IsDir() {
				continue
			}
			nameSplit := strings.Split(file.Name(), ".")
			if len(nameSplit) == 0 {
				continue
			}

			partitionPart := nameSplit[len(nameSplit)-1]
			partition, err := strconv.Atoi(partitionPart)
			if err != nil {
				log.Printf("cannot parse partition from name %s. Ignoring", file.Name())
				continue
			}
			parts = append(parts, int32(partition))
		}
	}

	if len(parts) == 0 {
		log.Fatalf("no partitions found")
	}

	var items int

	err = localstorage.IterateStorage(*baseFolder, *table, parts, func(partition int32, key, value []byte) error {
		if *printKeys {
			log.Printf("key: %s", string(key))
		}
		items++

		if *limit > 0 && items > *limit {
			return localstorage.ErrStop
		}
		return nil
	})

	if err != nil {
		log.Fatalf("error iterating storage: %v", err)
	}

	if *count {
		log.Printf("found %d items in total", items)
	}

}
