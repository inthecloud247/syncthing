package main

import (
	"os"
	"path"

	"github.com/calmh/syncthing/buffers"
)

type requestResult struct {
	node   string
	repo   string
	file   string // repo-relative name
	path   string // full path name, fs-normalized
	offset int64
	data   []byte
	err    error
}

type openFile struct {
	path         string // full path name, fs-normalized
	temp         string // temporary filename, full path, fs-normalized
	availability uint64 // availability bitset
	file         *os.File
	err          error // error when opening or writing to file, all following operations are cancelled
	outstanding  int   // number of requests we still have outstanding
	done         bool  // we have sent all requests for this file
}

type activityMap map[string]int

func (m activityMap) leastBusyNode(availability uint64, model *Model) string {
	var low int = 2<<31 - 1
	var selected string
	for node, usage := range m {
		if availability&1<<model.cm.Get(node) != 0 {
			if usage < low {
				low = usage
				selected = node
			}
		}
	}
	m[selected]++
	return selected
}

func (m activityMap) decrease(node string) {
	m[node]--
}

func (m *Model) puller(repo string, dir string, slots int) {
	var oustandingPerNode = make(activityMap)
	var openFiles = make(map[string]openFile)
	var requestSlots = make(chan bool, slots)
	var blocks = make(chan bqBlock)
	var requestResults = make(chan requestResult)

	for i := 0; i < slots; i++ {
		requestSlots <- true
	}

	go func() {
		// fill blocks queue when there are free slots
		for {
			<-requestSlots
			blocks <- m.bq.get()
		}
	}()

pull:
	for {
		select {
		case res := <-requestResults:
			oustandingPerNode.decrease(res.node)
			of, ok := openFiles[res.file]
			if !ok || of.err != nil {
				// no entry in openFiles means there was an error and we've cancelled the operation
				continue
			}
			_, of.err = of.file.WriteAt(res.data, res.offset)
			buffers.Put(res.data)
			of.outstanding--
			if of.done && of.outstanding == 0 {
				of.file.Close()
				delete(openFiles, res.file)
				// Hash check the file and rename
			}

		case b := <-blocks:
			f := b.file

			of, ok := openFiles[f.Name]
			if !ok {
				of.path = FSNormalize(path.Join(dir, f.Name))
				of.temp = FSNormalize(path.Join(dir, defTempNamer.TempName(f.Name)))
				of.availability = uint64(m.fs.Availability(f.Name))
				of.done = b.last

				of.file, of.err = os.OpenFile(of.temp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(f.Flags&0777))
				if of.err != nil {
					openFiles[f.Name] = of
					continue pull
				}
			}

			if len(b.copy) > 0 {
				// We have blocks to copy from the existing file
				var exfd *os.File
				exfd, of.err = os.Open(of.path)
				if of.err != nil {
					of.file.Close()
					openFiles[f.Name] = of
					continue pull
				}

				for _, b := range b.copy {
					bs := buffers.Get(int(b.Size))
					_, of.err = exfd.ReadAt(bs, b.Offset)
					if of.err == nil {
						_, of.err = of.file.WriteAt(bs, b.Offset)
					}
					buffers.Put(bs)
					if of.err != nil {
						exfd.Close()
						of.file.Close()
						openFiles[f.Name] = of
						continue pull
					}
				}

				exfd.Close()
			}

			if b.block.Size > 0 {
				of.outstanding--
				openFiles[f.Name] = of
				node := oustandingPerNode.leastBusyNode(of.availability, m)
				go func(node string, b bqBlock) {
					bs, err := m.protoConn[node].Request("default", f.Name, b.block.Offset, int(b.block.Size))
					requestResults <- requestResult{
						node:   node,
						repo:   "default",
						file:   f.Name,
						path:   of.path,
						offset: b.block.Offset,
						data:   bs,
						err:    err,
					}
					requestSlots <- true
				}(node, b)
			} else {
				// nothing more to do
				requestSlots <- true
			}
		}
	}
}
