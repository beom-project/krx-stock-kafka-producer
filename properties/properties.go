package properties

import (
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

type Properties struct {
	KafkaProperties KafkaProperties `yaml:"kafka"`
	MysqlProperties MysqlProperties `yaml:"mysql"`
}

type KafkaProperties struct {
	Addrs []string `yaml:"addrs"`
	Topic string   `yaml:"topic"`
}

type MysqlProperties struct {
	Addr     string `yaml:"addr"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	DBName   string `yaml:"dbname"`
}

func NewProperties() *Properties {
	return &Properties{}
}

func (p *Properties) SetProperties(file_name string) {
	properties := convertYamlToProperties(file_name)
	p.KafkaProperties = properties.KafkaProperties
	p.MysqlProperties = properties.MysqlProperties
}

func convertYamlToProperties(file_name string) *Properties {
	properties, err := ioutil.ReadFile(file_name)
	if err != nil {
		log.Fatal(err)
	}
	config := &Properties{}
	err = yaml.Unmarshal(properties, config)
	if err != nil {
		log.Fatal(err)
	}
	return config
}
