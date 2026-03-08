package lock

import (
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

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck,
		lockName: lockname}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// for {

	// 	ok, _, _ := lk.ck.Get(lk.lockName)

	// 	if ok != "" {
	// 		time.Sleep(10 * time.Millisecond)
	// 		continue
	// 	}

	// 	value := lk.clientId
	// 	lk.ck.Put(lk.lockName, value, 0)

	// 	k, _, _ := lk.ck.Get(lk.lockName)

	// 	if k == lk.clientId {
	// 		return
	// 	}

	// 	time.Sleep(10 * time.Millisecond)
	// }
}

func (lk *Lock) Release() {
	// lk.ck.Put(lk.lockName, "", 0)

}
