package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"

	"raftdb/internal/client"
	"raftdb/internal/engine"
	"raftdb/internal/server"
)

// parseCluster parses "1=host:port,2=host:port". Without IDs
// ("host:port,host:port"), nodes are numbered from 1 in order.
func parseCluster(spec string) (map[uint64]string, error) {
	addrs := map[uint64]string{}
	for i, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr := uint64(i+1), part
		if k, v, ok := strings.Cut(part, "="); ok {
			n, err := strconv.ParseUint(k, 10, 64)
			if err != nil || n == 0 {
				return nil, fmt.Errorf("bad node ID %q", k)
			}
			id, addr = n, v
		}
		if _, dup := addrs[id]; dup {
			return nil, fmt.Errorf("node %d listed twice", id)
		}
		addrs[id] = addr
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no nodes in %q", spec)
	}
	return addrs, nil
}

func serve(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	id := fs.Uint64("id", 0, "this node's ID")
	spec := fs.String("cluster", "", "every node: 1=host:port,2=host:port,...")
	dir := fs.String("dir", "", "data directory (default raftdb-node<ID>)")
	fs.Parse(args)
	addrs, err := parseCluster(*spec)
	if err != nil {
		fatal("serve:", err)
	}
	if _, ok := addrs[*id]; !ok {
		fatal("serve: -id must name one of the nodes in -cluster")
	}
	if *dir == "" {
		*dir = fmt.Sprintf("raftdb-node%d", *id)
	}
	p, err := server.Start(server.NodeConfig{ID: *id, Addrs: addrs, Dir: *dir})
	if err != nil {
		fatal("serve:", err)
	}
	fmt.Fprintf(os.Stderr, "node %d serving on %s (data in %s)\n", *id, addrs[*id], *dir)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		fmt.Fprintln(os.Stderr, "shutting down")
		p.Stop()
	}()
	p.Wait()
}

func connect(args []string) {
	fs := flag.NewFlagSet("connect", flag.ExitOnError)
	spec := fs.String("cluster", "127.0.0.1:7001", "the nodes' addresses: host:port,host:port,...")
	fs.Parse(args)
	addrs, err := parseCluster(*spec)
	if err != nil {
		fatal("connect:", err)
	}
	var ids []uint64
	for id := range addrs {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	var list []string
	for _, id := range ids {
		list = append(list, addrs[id])
	}
	c, err := client.Dial(list, client.Options{})
	if err != nil {
		fatal("connect:", err)
	}
	defer c.Close()
	s := c.Session()
	defer s.Close()
	runShell(remoteDB{s}, *spec)
}

type remoteDB struct{ s *client.Session }

func (r remoteDB) Exec(sql string) ([]*engine.Result, error) { return r.s.Exec(sql) }
func (r remoteDB) InTransaction() bool                       { return r.s.InTransaction() }
func (r remoteDB) Tables() ([]*engine.Table, error)          { return r.s.Tables() }
func (r remoteDB) Stats() (string, error) {
	st, leader, err := r.s.Status()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("node %d: %s, term %d, commit %d, applied %d; leader at %s",
		st.ID, st.Role, st.Term, st.Commit, st.Applied, leader), nil
}
