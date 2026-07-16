package hash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

type Ring struct {
	mu           sync.RWMutex
	virtualNodes int
	keys         []uint32
	ring         map[uint32]string
	nodes        map[string]struct{}
}

func NewRing(nodes []string, virtualNodes int) *Ring {
	if virtualNodes <= 0 {
		virtualNodes = 100
	}
	r := &Ring{
		virtualNodes: virtualNodes,
		ring:         make(map[uint32]string),
		nodes:        make(map[string]struct{}),
	}
	for _, node := range nodes {
		r.addNodeLocked(node)
	}
	r.sortLocked()
	return r
}

func (r *Ring) AddNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addNodeLocked(node)
	r.sortLocked()
}

func (r *Ring) RemoveNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[node]; !ok {
		return
	}
	delete(r.nodes, node)
	for i := 0; i < r.virtualNodes; i++ {
		hash := HashKey(node + "#" + strconv.Itoa(i))
		delete(r.ring, hash)
	}
	r.rebuildKeysLocked()
}

func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]string, 0, len(r.nodes))
	for node := range r.nodes {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	return nodes
}

func (r *Ring) Owner(key string) (string, bool) {
	owners := r.Owners(key, 1)
	if len(owners) == 0 {
		return "", false
	}
	return owners[0], true
}

func (r *Ring) Owners(key string, replicationFactor int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.keys) == 0 || replicationFactor <= 0 {
		return nil
	}
	if replicationFactor > len(r.nodes) {
		replicationFactor = len(r.nodes)
	}

	keyHash := HashKey(key)
	idx := sort.Search(len(r.keys), func(i int) bool {
		return r.keys[i] >= keyHash
	})
	if idx == len(r.keys) {
		idx = 0
	}

	owners := make([]string, 0, replicationFactor)
	seen := make(map[string]struct{}, replicationFactor)
	for scanned := 0; scanned < len(r.keys) && len(owners) < replicationFactor; scanned++ {
		node := r.ring[r.keys[(idx+scanned)%len(r.keys)]]
		if _, ok := seen[node]; ok {
			continue
		}
		seen[node] = struct{}{}
		owners = append(owners, node)
	}
	return owners
}

func HashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (r *Ring) addNodeLocked(node string) {
	if node == "" {
		return
	}
	if _, ok := r.nodes[node]; ok {
		return
	}
	r.nodes[node] = struct{}{}
	for i := 0; i < r.virtualNodes; i++ {
		hash := HashKey(node + "#" + strconv.Itoa(i))
		r.ring[hash] = node
		r.keys = append(r.keys, hash)
	}
}

func (r *Ring) sortLocked() {
	sort.Slice(r.keys, func(i, j int) bool {
		return r.keys[i] < r.keys[j]
	})
}

func (r *Ring) rebuildKeysLocked() {
	r.keys = r.keys[:0]
	for key := range r.ring {
		r.keys = append(r.keys, key)
	}
	r.sortLocked()
}
