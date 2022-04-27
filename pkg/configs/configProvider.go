package configs

import (
	"encoding/json"
	"io/ioutil"
)

const path = "/home/matt/jdb/pkg/configs/config.json"

var activeConfigs JDBConfig

func Load() error {
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	configs := JDBConfig{}

	json.Unmarshal(contents, &configs)
	activeConfigs = configs

	return nil
}

func GetConfigs() JDBConfig {
	return activeConfigs
}
