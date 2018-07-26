package util

// import (
//   "strings"
// )

type WeightedQuickUnion struct {
    id map[string]string
    sz map[string]int
}

func (wqu *WeightedQuickUnion) Connected(p, q string) bool {
    return wqu.findRoot(p) == wqu.findRoot(q)
}

func (wqu *WeightedQuickUnion) findRoot(p string) string {
    if _, exists := wqu.id[p];!exists {
      wqu.id[p] = p 
    }
    for p != wqu.id[p] {
        wqu.id[p] = wqu.id[wqu.id[p]]
        p = wqu.id[p]
    }
    return p
}
 
func (wqu *WeightedQuickUnion) Union(p, q string) bool {
    rp := wqu.findRoot(p)
    rq := wqu.findRoot(q)
    if rp == rq {
      return true
    }

    if _, exists := wqu.sz[rp]; !exists{
      wqu.sz[rp] = 1
    }

    if _, exists := wqu.sz[rq]; !exists{
      wqu.sz[rq] = 1
    }
  
    if wqu.sz[rp] < wqu.sz[rq] {
        wqu.id[rp] = rq
        wqu.sz[rq] += wqu.sz[rp]
    } else {
        wqu.id[rq] = rp
        wqu.sz[rp] += wqu.sz[rq]
    }
    return false
}
 
func NewWeightedQuickUnion() *WeightedQuickUnion {
    id := make(map[string]string)
    sz := make(map[string]int)
    return &WeightedQuickUnion{id:id, sz: sz}
}

