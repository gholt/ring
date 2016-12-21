package ring

import "testing"

func TestCanonicalHostPort(t *testing.T) {
	hostPort, err := CanonicalHostPort("1.2.3.4", 5678)
	if err != nil {
		t.Fatal(err)
	}
	if hostPort != "1.2.3.4:5678" {
		t.Fatal(hostPort)
	}

	hostPort, err = CanonicalHostPort("1.2.3.4:9012", 5678)
	if err != nil {
		t.Fatal(err)
	}
	if hostPort != "1.2.3.4:9012" {
		t.Fatal(hostPort)
	}

	hostPort, err = CanonicalHostPort("::1", 5678)
	if err != nil {
		t.Fatal(err)
	}
	if hostPort != "[::1]:5678" {
		t.Fatal(hostPort)
	}

	hostPort, err = CanonicalHostPort("[::1]", 5678)
	if err != nil {
		t.Fatal(err)
	}
	if hostPort != "[::1]:5678" {
		t.Fatal(hostPort)
	}

	hostPort, err = CanonicalHostPort("[::1]:9012", 5678)
	if err != nil {
		t.Fatal(err)
	}
	if hostPort != "[::1]:9012" {
		t.Fatal(hostPort)
	}
}
