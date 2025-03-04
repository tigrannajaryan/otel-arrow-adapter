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

package arrow2

import (
	"fmt"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"go.opentelemetry.io/collector/pdata/pmetric"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow2"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

var (
	ResourceMetricsDT = arrow.StructOf([]arrow.Field{
		{Name: constants.Resource, Type: acommon.ResourceDT, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.SchemaUrl, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Optional, schema.Dictionary)},
		{Name: constants.ScopeMetrics, Type: arrow.ListOf(ScopeMetricsDT), Metadata: schema.Metadata(schema.Optional)},
	}...)
)

// ResourceMetricsBuilder is a helper to build resource metrics.
type ResourceMetricsBuilder struct {
	released bool

	builder *builder.StructBuilder // builder for the resource metrics struct

	rb   *acommon.ResourceBuilder // resource builder
	schb *builder.StringBuilder   // schema url builder
	spsb *builder.ListBuilder     // scope metrics list builder
	smb  *ScopeMetricsBuilder     // scope metrics builder
}

// ResourceMetricsBuilderFrom creates a new ResourceMetricsBuilder from an existing builder.
func ResourceMetricsBuilderFrom(builder *builder.StructBuilder) *ResourceMetricsBuilder {
	spsb := builder.ListBuilder(constants.ScopeMetrics)
	return &ResourceMetricsBuilder{
		released: false,
		builder:  builder,
		rb:       acommon.ResourceBuilderFrom(builder.StructBuilder(constants.Resource)),
		schb:     builder.StringBuilder(constants.SchemaUrl),
		spsb:     spsb,
		smb:      ScopeMetricsBuilderFrom(spsb.StructBuilder()),
	}
}

// Build builds the resource metrics array.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the array.
func (b *ResourceMetricsBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, fmt.Errorf("resource metrics builder already released")
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Append appends a new resource metrics to the builder.
func (b *ResourceMetricsBuilder) Append(sm pmetric.ResourceMetrics) error {
	if b.released {
		return fmt.Errorf("resource metrics builder already released")
	}

	return b.builder.Append(sm, func() error {
		if err := b.rb.Append(sm.Resource()); err != nil {
			return err
		}
		b.schb.Append(sm.SchemaUrl())
		smetrics := sm.ScopeMetrics()
		sc := smetrics.Len()
		return b.spsb.Append(sc, func() error {
			for i := 0; i < sc; i++ {
				if err := b.smb.Append(smetrics.At(i)); err != nil {
					return err
				}
			}
			return nil
		})
	})
}

// Release releases the memory allocated by the builder.
func (b *ResourceMetricsBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}
