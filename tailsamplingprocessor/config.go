// SPDX-License-Identifier: Apache-2.0

package tailsamplingprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"

type Config struct {
	// SampleRate 是采样的目标比例，例如 0.1 代表 10%。
	// 对应 Python TracePicker 的 sampleRate
	SampleRate     float64     `mapstructure:"sample_rate"`
	UpdateInterval int         `mapstructure:"update_interval"`
	Kafka          KafkaConfig `mapstructure:"kafka"`
	Dynamic        bool        `mapstructure:"dynamic"`
}

type KafkaConfig struct {
	Brokers []string `mapstructure:"brokers"`
	Topic   string   `mapstructure:"topic"`
	Group   string   `mapstructure:"group"`
}
