package mapreduce

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)

type ByKey []KeyValue

func (this ByKey) Len() int {
	return len(this)
}

func (this ByKey) Less(i, j int) bool {
	return this[i].Key < this[j].Key
}

func (this ByKey) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//

	var kvs []KeyValue
	for i := 0; i < nMap; i++ {
		reduce_file := reduceName(jobName, i, reduceTask)
		reduce_file_fd, err := os.Open(reduce_file)
		if err != nil {
			fmt.Printf("open reduce file:%s failed", reduce_file)
		}
		decoder := json.NewDecoder(reduce_file_fd)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				fmt.Printf("decode failed, file name: %s", reduce_file)
				reduce_file_fd.Close()
				return
			}
			kvs = append(kvs, kv)
		}
		reduce_file_fd.Close()
	}

	sort.Sort(ByKey(kvs))

	output_file_fd, err := os.Create(outFile)
	if err != nil {
		fmt.Printf("error while creating out file: %s", outFile)
	}
	defer output_file_fd.Close()

	enc := json.NewEncoder(output_file_fd)
	kvs_len := len(kvs)
	if kvs_len == 0 {
		return
	}

	key := kvs[0].Key
	var values []string
	values = append(values, kvs[0].Value)
	for i := 1; i < kvs_len; i++ {
		if key != kvs[i].Key {
			enc.Encode(KeyValue{key, reduceF(key, values)})
			key = kvs[i].Key
			values = values[:0]
		}
		values = append(values, kvs[i].Value)
	}
	enc.Encode(KeyValue{key, reduceF(key, values)})
	return

	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
}
