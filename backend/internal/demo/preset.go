package demo

// Preset names demo workload bundles (non-production).
type Preset int

const (
	PresetUnknown Preset = iota
	PresetSmall
	PresetRetry
	PresetAbandonment
	PresetMixed
)

// PresetFromString maps CLI/HTTP names to presets.
func PresetFromString(s string) Preset {
	switch s {
	case "small-demo":
		return PresetSmall
	case "retry-demo":
		return PresetRetry
	case "abandonment-demo":
		return PresetAbandonment
	case "mixed-demo":
		return PresetMixed
	default:
		return PresetUnknown
	}
}

func (p Preset) String() string {
	switch p {
	case PresetSmall:
		return "small-demo"
	case PresetRetry:
		return "retry-demo"
	case PresetAbandonment:
		return "abandonment-demo"
	case PresetMixed:
		return "mixed-demo"
	default:
		return "unknown"
	}
}
