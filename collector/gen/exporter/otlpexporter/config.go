// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otlpexporter // import "github.com/f5/otel-arrow-adapter/collector/gen/exporter/otlpexporter"

import (
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Config defines configuration for OpenCensus exporter.
type Config struct {
	exporterhelper.TimeoutSettings `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.
	exporterhelper.QueueSettings   `mapstructure:"sending_queue"`
	exporterhelper.RetrySettings   `mapstructure:"retry_on_failure"`

	configgrpc.GRPCClientSettings `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.

	Arrow ArrowSettings `mapstructure:"arrow"`
}

// ArrowSettings includes whether Arrow is enabled and the number of
// concurrent Arrow streams.
type ArrowSettings struct {
	Enabled          bool `mapstructure:"enabled"`
	NumStreams       int  `mapstructure:"num_streams"`
	DisableDowngrade bool `mapstructure:"disable_downgrade"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the exporter configuration is valid
func (cfg *Config) Validate() error {
	if err := cfg.QueueSettings.Validate(); err != nil {
		return fmt.Errorf("queue settings has invalid configuration: %w", err)
	}
	if err := cfg.Arrow.Validate(); err != nil {
		return fmt.Errorf("arrow settings has invalid configuration: %w", err)
	}

	return nil
}

// Validate returns an error when the number of streams is less than 1.
func (cfg *ArrowSettings) Validate() error {
	if cfg.NumStreams < 1 {
		return fmt.Errorf("stream count must be > 0: %d", cfg.NumStreams)
	}

	return nil
}
