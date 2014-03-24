package main

import "github.com/calmh/syncthing/scanner"

type bqAdd struct {
	file scanner.File
	have []scanner.Block
	need []scanner.Block
}

type bqBlock struct {
	file  scanner.File
	block scanner.Block   // get this block from the network
	copy  []scanner.Block // copy these blocks from the old version of the file
	last  bool
}

type blockQueue struct {
	inbox  chan bqAdd
	outbox chan bqBlock

	queued []bqBlock
}

func newBlockQueue() *blockQueue {
	q := &blockQueue{
		inbox:  make(chan bqAdd),
		outbox: make(chan bqBlock),
	}
	go q.run()
	return q
}

func (q *blockQueue) addBlock(a bqAdd) {
	// If we already have it queued, return
	for _, b := range q.queued {
		if b.file.Name == a.file.Name {
			return
		}
	}
	if len(a.have) > 0 {
		// First queue a copy operation
		q.queued = append(q.queued, bqBlock{
			file: a.file,
			copy: a.have,
		})
	}
	// Queue the needed blocks individually
	l := len(a.need)
	for i, b := range a.need {
		q.queued = append(q.queued, bqBlock{
			file:  a.file,
			block: b,
			last:  i == l-1,
		})
	}
}

func (q *blockQueue) run() {
	for {
		if len(q.queued) == 0 {
			q.addBlock(<-q.inbox)
		} else {
			next := q.queued[0]
			select {
			case a := <-q.inbox:
				q.addBlock(a)
			case q.outbox <- next:
				q.queued = q.queued[1:]
			}
		}
	}
}

func (q *blockQueue) put(a bqAdd) {
	q.inbox <- a
}

func (q *blockQueue) get() bqBlock {
	return <-q.outbox
}
