package cmap

import (
	"encoding/json"
	"sync"
	"time"
)

//var SHARD_COUNT = 32

// A "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (SHARD_COUNT) map shards.
type ConcurrentMapWithExpire struct {
	coMap []*ConcurrentMapSharedSimple

	coLoad func(key string) (interface{}, *time.Duration, error)

	expiration *time.Duration
	clock      Clock
}

// A "thread" safe string to anything map.
type ConcurrentMapSharedSimple struct {
	items        map[string]*simpleItem
	sync.RWMutex // Read Write mutex, guards access to internal map.

}

type simpleItem struct {
	clock      Clock
	value      interface{}
	expiration *time.Time
}

func (si *simpleItem) IsExpired(now *time.Time) bool {
	if si.expiration == nil {
		return false
	}
	if now == nil {
		t := si.clock.Now()
		now = &t
	}
	return si.expiration.Before(*now)
}

// Creates a new concurrent map.
func NewA() ConcurrentMapWithExpire {
	m := ConcurrentMapWithExpire{coMap: make([]*ConcurrentMapSharedSimple, SHARD_COUNT), coLoad: nil}
	for i := 0; i < SHARD_COUNT; i++ {
		m.coMap[i] = &ConcurrentMapSharedSimple{items: make(map[string]*simpleItem)}
	}
	return m
}

func NewAV2(coLoad func(key string) (interface{}, *time.Duration, error), ex *time.Duration) ConcurrentMapWithExpire {
	m := ConcurrentMapWithExpire{coMap: make([]*ConcurrentMapSharedSimple, SHARD_COUNT), coLoad: coLoad}
	for i := 0; i < SHARD_COUNT; i++ {
		m.coMap[i] = &ConcurrentMapSharedSimple{items: make(map[string]*simpleItem)}
	}
	m.expiration = ex
	m.clock = NewRealClock()
	return m
}

// Returns shard under given key
func (m ConcurrentMapWithExpire) GetShard(key string) *ConcurrentMapSharedSimple {
	return m.coMap[uint(fnv32A(key))%uint(SHARD_COUNT)]
}

func (m ConcurrentMapWithExpire) MSet(data map[string]interface{}) {
	for key, value := range data {
		m.Set(key, value)
	}
}

// Sets the given value under the specified key.
func (m ConcurrentMapWithExpire) Set(key string, value interface{}) {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	item := &simpleItem{value: value, clock: m.clock}
	if m.expiration != nil {
		t := item.clock.Now().Add(*m.expiration)
		item.expiration = &t
	}
	shard.items[key] = item
	shard.Unlock()
}

// Callback to return new element to be inserted into the map
// It is called while lock is held, therefore it MUST NOT
// try to access other keys in same map, as it can lead to deadlock since
// Go sync.RWLock is not reentrant
type UpsertCbA func(exist bool, valueInMap interface{}, newValue interface{}) interface{}

// Insert or Update - updates existing element or inserts a new one using UpsertCb
func (m ConcurrentMapWithExpire) Upsert(key string, value interface{}, cb UpsertCbA) (res interface{}) {
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	res = cb(ok, v, value)

	item := &simpleItem{value: res, clock: m.clock}
	if m.expiration != nil {
		t := item.clock.Now().Add(*m.expiration)
		item.expiration = &t
	}
	shard.items[key] = item

	shard.Unlock()
	return res
}

// Sets the given value under the specified key if no value was associated with it.
func (m ConcurrentMapWithExpire) SetIfAbsent(key string, value interface{}) bool {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	_, ok := shard.items[key]
	if !ok {
		item := &simpleItem{value: value, clock: m.clock}
		if m.expiration != nil {
			t := item.clock.Now().Add(*m.expiration)
			item.expiration = &t
		}
	}
	shard.Unlock()
	return !ok
}

// Retrieves an element from map under given key.
func (m ConcurrentMapWithExpire) Get(key string) (interface{}, bool) {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// Get item from shard.
	val, ok := shard.items[key]
	// 过期时间判断 todo
	if ok && !val.IsExpired(nil) {
		shard.RUnlock()
		return val.value, true
	}

	shard.RUnlock()
	return nil, false
}

// Retrieves an element from map under given key.
func (m ConcurrentMapWithExpire) GetV2(key string) (interface{}, error) {
	var err error
	var ex *time.Duration
	val, ok := m.Get(key)
	if !ok {
		if m.coLoad != nil {
			shard := m.GetShard(key)
			shard.Lock()
			// 再判断一次
			if val, ok := shard.items[key]; ok {
				if !val.IsExpired(nil) {
					shard.Unlock()
					return val.value, nil
				}
			}
			// 触发加载
			val, ex, err = m.coLoad(key)
			if err == nil {
				item := &simpleItem{value: val, clock: m.clock}
				if ex != nil {
					t := item.clock.Now().Add(*ex)
					item.expiration = &t
				} else if m.expiration != nil {
					t := item.clock.Now().Add(*m.expiration)
					item.expiration = &t
				}
				shard.items[key] = item
			}
			shard.Unlock()
		}
	}
	return val, err
}

// Returns the number of elements within the map.
func (m ConcurrentMapWithExpire) Count() int {
	count := 0
	for i := 0; i < SHARD_COUNT; i++ {
		shard := m.coMap[i]
		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

// Looks up an item under specified key
// 这里没有判断过期情况
func (m ConcurrentMapWithExpire) Has(key string) bool {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// See if element is within shard.
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

// Removes an element from the map.
func (m ConcurrentMapWithExpire) Remove(key string) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

// RemoveCb is a callback executed in a map.RemoveCb() call, while Lock is held
// If returns true, the element will be removed from the map
type RemoveCbA func(key string, v interface{}, exists bool) bool

// RemoveCb locks the shard containing the key, retrieves its current value and calls the callback with those params
// If callback returns true and element exists, it will remove it from the map
// Returns the value returned by the callback (even if element was not present in the map)
func (m ConcurrentMapWithExpire) RemoveCbA(key string, cb RemoveCbA) bool {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	remove := cb(key, v, ok)
	if remove && ok {
		delete(shard.items, key)
	}
	shard.Unlock()
	return remove
}

// Removes an element from the map and returns it
func (m ConcurrentMapWithExpire) Pop(key string) (v interface{}, exists bool) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()
	return v, exists
}

// Checks if map is empty.
func (m ConcurrentMapWithExpire) IsEmpty() bool {
	return m.Count() == 0
}

// Used by the Iter & IterBuffered functions to wrap two variables together over a channel,
type TupleA struct {
	Key string
	Val *simpleItem
}

// Returns an iterator which could be used in a for range loop.
//
// Deprecated: using IterBuffered() will get a better performence
func (m ConcurrentMapWithExpire) Iter() <-chan TupleA {
	chans := snapshotA(m)
	ch := make(chan TupleA)
	go fanInA(chans, ch)
	return ch
}

// Returns a buffered iterator which could be used in a for range loop.
func (m ConcurrentMapWithExpire) IterBuffered() <-chan TupleA {
	chans := snapshotA(m)
	total := 0
	for _, c := range chans {
		total += cap(c)
	}
	ch := make(chan TupleA, total)
	go fanInA(chans, ch)
	return ch
}

// Returns a array of channels that contains elements in each shard,
// which likely takes a snapshot of `m`.
// It returns once the size of each buffered channel is determined,
// before all the channels are populated using goroutines.
func snapshotA(m ConcurrentMapWithExpire) (chans []chan TupleA) {
	chans = make([]chan TupleA, SHARD_COUNT)
	wg := sync.WaitGroup{}
	wg.Add(SHARD_COUNT)
	// Foreach shard.
	for index, shard := range m.coMap {
		go func(index int, shard *ConcurrentMapSharedSimple) {
			// Foreach key, value pair.
			shard.RLock()
			chans[index] = make(chan TupleA, len(shard.items))
			wg.Done()
			for key, val := range shard.items {
				chans[index] <- TupleA{key, val}
			}
			shard.RUnlock()
			close(chans[index])
		}(index, shard)
	}
	wg.Wait()
	return chans
}

// fanIn reads elements from channels `chans` into channel `out`
func fanInA(chans []chan TupleA, out chan TupleA) {
	wg := sync.WaitGroup{}
	wg.Add(len(chans))
	for _, ch := range chans {
		go func(ch chan TupleA) {
			for t := range ch {
				out <- t
			}
			wg.Done()
		}(ch)
	}
	wg.Wait()
	close(out)
}

// Returns all items as map[string]interface{}
func (m ConcurrentMapWithExpire) Items() map[string]interface{} {
	tmp := make(map[string]interface{})

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val.value
	}

	return tmp
}

func (m ConcurrentMapWithExpire) Purge() {
	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		if item.Val.IsExpired(nil) {
			m.Remove(item.Key)
		}
	}

}

// Iterator callback,called for every key,value found in
// maps. RLock is held for all calls for a given shard
// therefore callback sess consistent view of a shard,
// but not across the shards
type IterCbA func(key string, v interface{})

// Callback based iterator, cheapest way to read
// all elements in a map.
func (m ConcurrentMapWithExpire) IterCb(fn IterCbA) {
	for idx := range m.coMap {
		shard := (m.coMap)[idx]
		shard.RLock()
		for key, value := range shard.items {
			fn(key, value)
		}
		shard.RUnlock()
	}
}

// Return all keys as []string
func (m ConcurrentMapWithExpire) Keys() []string {
	count := m.Count()
	ch := make(chan string, count)
	go func() {
		// Foreach shard.
		wg := sync.WaitGroup{}
		wg.Add(SHARD_COUNT)
		for _, shard := range m.coMap {
			go func(shard *ConcurrentMapSharedSimple) {
				// Foreach key, value pair.
				shard.RLock()
				for key := range shard.items {
					ch <- key
				}
				shard.RUnlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()

	// Generate keys
	keys := make([]string, 0, count)
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}

//Reviles ConcurrentMap "private" variables to json marshal.
func (m ConcurrentMapWithExpire) MarshalJSON() ([]byte, error) {
	// Create a temporary map, which will hold all item spread across shards.
	tmp := make(map[string]interface{})

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}
	return json.Marshal(tmp)
}

func fnv32A(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// Concurrent map uses Interface{} as its value, therefor JSON Unmarshal
// will probably won't know which to type to unmarshal into, in such case
// we'll end up with a value of type map[string]interface{}, In most cases this isn't
// out value type, this is why we've decided to remove this functionality.

// func (m *ConcurrentMap) UnmarshalJSON(b []byte) (err error) {
// 	// Reverse process of Marshal.

// 	tmp := make(map[string]interface{})

// 	// Unmarshal into a single map.
// 	if err := json.Unmarshal(b, &tmp); err != nil {
// 		return nil
// 	}

// 	// foreach key,value pair in temporary map insert into our concurrent map.
// 	for key, val := range tmp {
// 		m.Set(key, val)
// 	}
// 	return nil
// }
