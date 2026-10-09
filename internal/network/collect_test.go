package network

import "testing"

func TestIsNetworkInterface(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"eth0", true},
		{"enp1s0", true},
		{"wlan0", true},
		{"lo", false},
		{"docker0", false},
		{"veth123", false},
		{"br-123", false},
		{"vmnet1", false},
		{"unknown0", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isNetworkInterface(tc.name); got != tc.want {
				t.Fatalf("isNetworkInterface(%q) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}
