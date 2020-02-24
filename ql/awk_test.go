package ql

import "testing"

func TestNewAwkQuery(t *testing.T) {
	aw, err := NewAWKQuery(`
  BEGIN {count = 0}
	// {count++; setLabel("count", count); print $0}
	`, " ")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := 0; i < 10; i++ {
		nextValue := aw.Next()
		if !nextValue {
			t.Fatalf("Next returned %v", nextValue)
		}
		t.Logf("msg: %#v", aw.Message())
	}
}
