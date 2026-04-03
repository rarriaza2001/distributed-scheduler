package demo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPresetFromString_Table(t *testing.T) {
	tests := []struct {
		in  string
		out Preset
	}{
		{"small-demo", PresetSmall},
		{"retry-demo", PresetRetry},
		{"abandonment-demo", PresetAbandonment},
		{"mixed-demo", PresetMixed},
		{"", PresetUnknown},
		{"nope", PresetUnknown},
	}
	for _, tc := range tests {
		require.Equal(t, tc.out, PresetFromString(tc.in), tc.in)
	}
}
