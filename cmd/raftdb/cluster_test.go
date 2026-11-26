package main

import (
	"maps"
	"testing"
)

func TestParseCluster(t *testing.T) {
	good := map[string]map[uint64]string{
		"1=a:1,2=b:2,3=c:3": {1: "a:1", 2: "b:2", 3: "c:3"},
		"a:1, b:2":          {1: "a:1", 2: "b:2"},
		"7=h:7,":            {7: "h:7"},
		"2=b:2,1=a:1":       {1: "a:1", 2: "b:2"},
	}
	for spec, want := range good {
		got, err := parseCluster(spec)
		if err != nil || !maps.Equal(got, want) {
			t.Errorf("parseCluster(%q) = %v, %v; want %v", spec, got, err, want)
		}
	}
	for _, spec := range []string{"", ",", "x=a:1", "0=a:1", "1=a:1,1=b:2"} {
		if got, err := parseCluster(spec); err == nil {
			t.Errorf("parseCluster(%q) = %v, want an error", spec, got)
		}
	}
}
