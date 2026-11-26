package cluster

import (
	"fmt"
	"testing"
)

// BenchmarkReplicatedInsert measures an autocommit INSERT on a three-node
// cluster: a ReadIndex round for the snapshot, then a proposal that the
// leader and a follower must fsync before it commits.
func BenchmarkReplicatedInsert(b *testing.B) {
	c := newTestCluster(b, 3)
	db := c.dbs[c.leader()]
	if _, err := db.Exec("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)"); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := range b.N {
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO t VALUES (%d, 'value')", i)); err != nil {
			b.Fatal(err)
		}
	}
}
