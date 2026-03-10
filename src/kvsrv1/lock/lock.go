package lock

import (
	"math/rand"
	"strconv"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	lockName string
	clientId string
	// You may add code here
}

// var clientCounter int
// var clientMu sync.Mutex

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	// clientMu.Lock()
	// clientCounter++
	// id := clientCounter
	// clientMu.Unlock()
	rand.Seed(time.Now().UnixNano())

	lk := &Lock{ck: ck,
		lockName: "lock-" + lockname,
		clientId: "client-" + strconv.Itoa(rand.Int()),
	}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	for {

		value, version, err := lk.ck.Get(lk.lockName)

		if err == rpc.ErrNoKey {
			version = 0
			value = ""
		}

		if value != "" {
			time.Sleep(8 * time.Millisecond)
			continue
		}

		err = lk.ck.Put(lk.lockName, lk.clientId, version)
		if err == rpc.OK {
			return
		}
		if err == rpc.ErrMaybe {
			v, _, err2 := lk.ck.Get(lk.lockName)
			if err2 == rpc.OK && v == lk.clientId {
				return
			}
		}
		time.Sleep(8 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		value, version, err := lk.ck.Get(lk.lockName)
		if err != rpc.OK || value != lk.clientId {
			return
		}

		err = lk.ck.Put(lk.lockName, "", version)
		if err == rpc.OK {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}
}
