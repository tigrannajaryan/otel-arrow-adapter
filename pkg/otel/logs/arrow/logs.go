/*
 * Copyright The OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package arrow

import (
	"fmt"

	"github.com/apache/arrow/go/v11/arrow"
	"go.opentelemetry.io/collector/pdata/plog"

	schema "github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

// Schema is the Arrow schema for the OTLP Arrow Logs record.
var (
	Schema = arrow.NewSchema([]arrow.Field{
		{Name: constants.ResourceLogs, Type: arrow.ListOf(ResourceLogsDT), Metadata: schema.Metadata(schema.Optional)},
	}, nil)
)

// LogsBuilder is a helper to build a list of resource logs.
type LogsBuilder struct {
	released bool

	builder *builder.RecordBuilderExt // Record builder
	rlb     *builder.ListBuilder      // ResourceLogs list builder
	rlp     *ResourceLogsBuilder      // resource logs builder
}

// NewLogsBuilder creates a new LogsBuilder with a given allocator.
func NewLogsBuilder(recordBuilder *builder.RecordBuilderExt) (*LogsBuilder, error) {
	b := &LogsBuilder{
		released: false,
		builder:  recordBuilder,
	}
	if err := b.init(); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *LogsBuilder) init() error {
	rlb := b.builder.ListBuilder(constants.ResourceLogs)
	b.rlb = rlb
	b.rlp = ResourceLogsBuilderFrom(rlb.StructBuilder())
	return nil
}

// Build builds an Arrow Record from the builder.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the record.
func (b *LogsBuilder) Build() (record arrow.Record, err error) {
	if b.released {
		return nil, fmt.Errorf("resource logs builder already released")
	}

	record, err = b.builder.NewRecord()
	if err != nil {
		initErr := b.init()
		if initErr != nil {
			err = initErr
		}
	}

	return
}

// Append appends a new set of resource logs to the builder.
func (b *LogsBuilder) Append(logs plog.Logs) error {
	if b.released {
		return fmt.Errorf("traces builder already released")
	}

	rl := logs.ResourceLogs()
	rc := rl.Len()
	return b.rlb.Append(rc, func() error {
		for i := 0; i < rc; i++ {
			if err := b.rlp.Append(rl.At(i)); err != nil {
				return err
			}
		}
		return nil
	})
}

// Release releases the memory allocated by the builder.
func (b *LogsBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}
