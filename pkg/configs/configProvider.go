package configs

import (
	"io/ioutil"
	"strings"
)

const path = "/home/matt/jdb/pkg/configs/openjdbconfig"

const (
	//Base local path for OpenJDB to store table files
	BaseDirectoryPath = "openjdb.base.directory"
)

var configs map[string]string = map[string]string{
	BaseDirectoryPath: "./",
}

func Load() error {
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	for _, line := range strings.Split(string(contents), "\n") {
		spl := strings.Split(line, "=")
		field, value := spl[0], spl[1]

		configs[field] = value
	}
	return nil
}

func GetConfig(name string) string {
	return configs[name]
}
