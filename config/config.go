// Copyright 2016 The MSP Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package config

import (
	"io/ioutil"
	"os"

	"github.com/compasses/mysql-smart-proxy/core/yaml"
)

//整个config文件对应的结构
type Config struct {
	Addr        string       `yaml:"addr"`
	User        string       `yaml:"user"`
	Password    string       `yaml:"password"`
	LogPath     string       `yaml:"log_path"`
	LogLevel    string       `yaml:"log_level"`
	LogSql      int          `yaml:"log_sql"`
	SlowLogTime int64        `yaml:"slow_log_time"`
	Nodes       []NodeConfig `yaml:"nodes"`
}

var WorkMode = map[int]string{
	0: "Master Slave",
	1: "Cluster Peer",
}

//node节点对应的配置
type NodeConfig struct {
	Name             string `yaml:"name"`
	DownAfterNoAlive int    `yaml:"down_after_noalive"`
	MaxConnNum       int    `yaml:"max_conns_limit"`
	WorkMode         int    `yaml:"work_mode"` //0--master slave; 1 -- cluster peer node

	User     string `yaml:"user"`
	Password string `yaml:"password"`

	Master string `yaml:"master"`
	Slave  string `yaml:"slave"`
}

func ParseConfigData(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal([]byte(data), &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func ParseConfigFile(fileName string) (*Config, error) {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	return ParseConfigData(data)
}

func WriteConfigFile(cfg *Config) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}

	execPath, err := os.Getwd()
	if err != nil {
		return err
	}

	configPath := execPath + "/etc/ks.yaml"
	err = ioutil.WriteFile(configPath, data, 0755)
	if err != nil {
		return err
	}

	return nil
}
