package entity

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuthDecode(t *testing.T) {
	type Case struct {
		src      string
		keys     []string
		expected interface{}
	}

	cases := []Case{
		Case{
			src:      `{"foo":"{\"bar\":1}"}`,
			keys:     []string{"foo"},
			expected: []byte(`{"foo":{"bar":1}}`),
		},
		Case{
			src:      `{"foo":"{\"bar\":1}"}`,
			keys:     []string{"xxx"},
			expected: ([]byte)(nil),
		},
		Case{
			src:      `{"foo":{"bar":1}}`,
			keys:     []string{"foo"},
			expected: ([]byte)(nil),
		},
	}

	for _, c := range cases {
		data := []byte(c.src)
		fixed, err := autoDecode(data, c.keys)

		require.NoError(t, err)
		require.Equal(t, c.expected, fixed)
	}
}
