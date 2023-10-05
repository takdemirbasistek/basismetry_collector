package common

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

type ResponseMessage struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data"`
	Message string      `json:"message"`
}

func ReplyUtil(success bool, data interface{}, message string) ResponseMessage {
	return ResponseMessage{success, data, message}
}

func GetPathSeparator() string {
	osType := runtime.GOOS
	pathSeparator := "/"
	if strings.Contains(osType, "Windows") || strings.Contains(osType, "windows") {
		pathSeparator = "\\"
	}
	return pathSeparator
}

func GetEnvFilePath() string {
	pathDir, _ := os.Getwd()
	pathDirReplacer := strings.NewReplacer("\\main", "", "/main", "", "\\cmd", "", "/cmd", "")
	pathDir = pathDirReplacer.Replace(pathDir)
	osType := runtime.GOOS
	var file string
	if strings.Contains(osType, "windows") {
		file = filepath.Join(pathDir, "\\.env.win")
	} else {
		file = filepath.Join(pathDir, ".env")
	}
	return file
}
