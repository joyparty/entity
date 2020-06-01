package entity

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecursiveDecode(t *testing.T) {
	type Case struct {
		src      string
		keys     []string
		expected interface{}
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
			expected: ([]byte)(nil),
		},
		{
			src:      `{"foo":{"bar":1}}`,
			keys:     []string{"foo"},
			expected: ([]byte)(nil),
		},
	}

	for _, c := range cases {
		data := []byte(c.src)
		fixed, err := recursiveDecode(data, c.keys)

		require.NoError(t, err)
		require.Equal(t, c.expected, fixed)
	}
}
