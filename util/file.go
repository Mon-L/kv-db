package util

import (
	"os"
)

func CopyFile(srcFile string, destFile string) error {
	if _, err := os.Stat(srcFile); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return os.Rename(srcFile, destFile)
}
