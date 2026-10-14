//go:build !windows

package egress

import (
	"errors"
	"syscall"
)

func warpPIDExited(pid int) (bool, error) {
	err := syscall.Kill(pid, 0)
	if errors.Is(err, syscall.ESRCH) {
		return true, nil
	}
	return false, err
}
