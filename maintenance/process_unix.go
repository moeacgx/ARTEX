//go:build !windows

package maintenance

import (
	"errors"
	"syscall"
)

func hostProcessExited(pid int) (bool, error) {
	err := syscall.Kill(pid, 0)
	if errors.Is(err, syscall.ESRCH) {
		return true, nil
	}
	return false, err
}
