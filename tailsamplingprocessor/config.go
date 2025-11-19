// SPDX-License-Identifier: Apache-2.0

package tailsamplingprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"

type Config struct {
	// SampleRate 是采样的目标比例，例如 0.1 代表 10%。
	// 对应 Python TracePicker 的 sampleRate
	SampleRate     float64          `mapstructure:"sample_rate"`
	BatchSize      uint64           `mapstructure:"batch_size"`
	UpdateInterval int              `mapstructure:"update_interval"`
	Kafka          KafkaConfig      `mapstructure:"kafka"`
	Dynamic        bool             `mapstructure:"dynamic"`
	Mode           string           `mapstructure:"mode"`
	Controller     ControllerConfig `mapstructure:"controller"`
}

type KafkaConfig struct {
	Brokers []string `mapstructure:"brokers"`
	Topic   string   `mapstructure:"topic"`
	Group   string   `mapstructure:"group"`
}

type ControllerConfig struct {
	Kp float64 `mapstructure:"kp"`
	Ki float64 `mapstructure:"ki"`
	Kd float64 `mapstructure:"kd"`

	BufferSize   int     `mapstructure:"buffer_size"`
	Alpha        float64 `mapstructure:"alpha"`
	LagThreshold int64   `mapstructure:"lag_threshold"`
	LagBase      int64   `mapstructure:"lag_base"`

	ShrinkThreshold int64   `mapstructure:"shrink_threshold"`
	ShrinkFactor    float64 `mapstructure:"shrink_factor"`
}
