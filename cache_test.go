package entity

import (
	"bytes"
	"testing"
)

func TestRecursiveDecode(t *testing.T) {
	type Case struct {
		src      string
		keys     []string
		expected []byte
	}

	cases := []Case{
		{
			src:      `{"foo":"{\"bar\":1}"}`,
			keys:     []string{"foo"},
			expected: []byte(`{"foo":{"bar":1}}`),
		},
		{
			src:      `{"foo":"{\"bar\":1}"}`,
			keys:     []string{"xxx"},
			expected: nil,
		},
		{
			src:      `{"foo":{"bar":1}}`,
			keys:     []string{"foo"},
			expected: nil,
		},
	}

	for _, c := range cases {
		data := []byte(c.src)
		fixed, err := recursiveDecode(data, c.keys)
		if err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(fixed, c.expected) {
			t.Fatalf("expected %s, got %s", c.expected, fixed)
		}
	}
}
