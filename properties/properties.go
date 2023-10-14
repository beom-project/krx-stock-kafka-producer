package properties

import (
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

type Properties struct {
	KafkaProperties KafkaProperties `yaml:"kafka"`
}

type KafkaProperties struct {
	Addrs []string `yaml:"addrs"`
	Topic string   `yaml:"topic"`
}

func NewProperties() *Properties {
	return &Properties{}
}

func (p *Properties) SetProperties(file_name string) {
	properties := convertYamlToProperties(file_name)
	p.KafkaProperties = properties.KafkaProperties
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
