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
	"go.opentelemetry.io/collector/pdata/ptrace"

	schema "github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

// Schema is the Arrow schema for the OTLP Arrow Traces record.
var (
	Schema = arrow.NewSchema([]arrow.Field{
		{Name: constants.ResourceSpans, Type: arrow.ListOf(ResourceSpansDT), Metadata: schema.Metadata(schema.Optional)},
	}, nil)
)

// TracesBuilder is a helper to build a list of resource spans.
type TracesBuilder struct {
	released bool

	builder *builder.RecordBuilderExt // Record builder
	rsb     *builder.ListBuilder      // Resource spans list builder
	rsp     *ResourceSpansBuilder     // resource spans builder
}

// NewTracesBuilder creates a new TracesBuilder with a given allocator.
func NewTracesBuilder(rBuilder *builder.RecordBuilderExt) (*TracesBuilder, error) {
	tracesBuilder := &TracesBuilder{
		released: false,
		builder:  rBuilder,
	}
	if err := tracesBuilder.init(); err != nil {
		return nil, err
	}
	return tracesBuilder, nil
}

func (b *TracesBuilder) init() error {
	rsb := b.builder.ListBuilder(constants.ResourceSpans)
	b.rsb = rsb
	b.rsp = ResourceSpansBuilderFrom(rsb.StructBuilder())
	return nil
}

// Build builds an Arrow Record from the builder.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the record.
//
// This method returns a DictionaryOverflowError if the cardinality of a dictionary
// (or several) exceeds the maximum allowed value.
func (b *TracesBuilder) Build() (record arrow.Record, err error) {
	if b.released {
		return nil, fmt.Errorf("resource spans builder already released")
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

// Append appends a new set of resource spans to the builder.
func (b *TracesBuilder) Append(traces ptrace.Traces) error {
	if b.released {
		return fmt.Errorf("traces builder already released")
	}

	rs := traces.ResourceSpans()
	rc := rs.Len()
	return b.rsb.Append(rc, func() error {
		for i := 0; i < rc; i++ {
			if err := b.rsp.Append(rs.At(i)); err != nil {
				return err
			}
		}
		return nil
	})
}

// Release releases the memory allocated by the builder.
func (b *TracesBuilder) Release() {
	if !b.released {
		b.builder.Release()
		b.released = true
	}
}
