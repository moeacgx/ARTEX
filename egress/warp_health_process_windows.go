package egress

import (
	"errors"

	"golang.org/x/sys/windows"
)

func warpPIDExited(pid int) (bool, error) {
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.SYNCHRONIZE, false, uint32(pid))
	if errors.Is(err, windows.ERROR_INVALID_PARAMETER) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer windows.CloseHandle(handle)
	// 进程句柄变为有信号才表示退出，退出码 259 也不能误判为仍运行。
	state, err := windows.WaitForSingleObject(handle, 0)
	return state == windows.WAIT_OBJECT_0, err
}
