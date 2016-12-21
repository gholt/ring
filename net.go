package ring

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

func CanonicalHostPort(hostport string, defaultPort int) (string, error) {
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		// SplitHostPort gives an error for a missing port, so we'll just
		// assume that and tack on the port and try again.
		host, port, err = net.SplitHostPort(fmt.Sprintf("%s:%d", hostport, defaultPort))
		if err != nil {
			// It could also be that the host is IPv6 but the user didn't
			// include the square brackets because they'd left out the port,
			// so...
			host, port, err = net.SplitHostPort(fmt.Sprintf("[%s]:%d", hostport, defaultPort))
			if err != nil {
				// Okay, at this point we can give up.
				return "", err
			}
		}
	}
	ip := net.ParseIP(host)
	if ip != nil {
		host = ip.String()
		if strings.Contains(host, ":") {
			host = "[" + host + "]"
		}
	}
	portInt, err := strconv.ParseInt(port, 10, 64)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, portInt), nil
}
