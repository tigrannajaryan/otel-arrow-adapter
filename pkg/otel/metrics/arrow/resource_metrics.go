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

package arrow

import (
	"fmt"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pmetric"

	acommon "github.com/f5/otel-arrow-adapter/pkg/otel/common/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

var (
	ResourceMetricsDT = arrow.StructOf([]arrow.Field{
		{Name: constants.Resource, Type: acommon.ResourceDT},
		{Name: constants.SchemaUrl, Type: acommon.DefaultDictString},
		{Name: constants.ScopeMetrics, Type: arrow.ListOf(ScopeMetricsDT)},
	}...)
)

// ResourceMetricsBuilder is a helper to build resource metrics.
type ResourceMetricsBuilder struct {
	released bool

	builder *array.StructBuilder // builder for the resource metrics struct

	rb   *acommon.ResourceBuilder           // resource builder
	schb *acommon.AdaptiveDictionaryBuilder // schema url builder
	spsb *array.ListBuilder                 // scope metrics list builder
	smb  *ScopeMetricsBuilder               // scope metrics builder
}

// NewResourceMetricsBuilder creates a new ResourceMetricsBuilder with a given allocator.
//
// Once the builder is no longer needed, Build() or Release() must be called to free the
// memory allocated by the builder.
func NewResourceMetricsBuilder(pool memory.Allocator) *ResourceMetricsBuilder {
	builder := array.NewStructBuilder(pool, ResourceMetricsDT)
	return ResourceMetricsBuilderFrom(builder)
}

// ResourceMetricsBuilderFrom creates a new ResourceMetricsBuilder from an existing builder.
func ResourceMetricsBuilderFrom(builder *array.StructBuilder) *ResourceMetricsBuilder {
	return &ResourceMetricsBuilder{
		released: false,
		builder:  builder,
		rb:       acommon.ResourceBuilderFrom(builder.FieldBuilder(0).(*array.StructBuilder)),
		schb:     acommon.AdaptiveDictionaryBuilderFrom(builder.FieldBuilder(1)),
		spsb:     builder.FieldBuilder(2).(*array.ListBuilder),
		smb:      ScopeMetricsBuilderFrom(builder.FieldBuilder(2).(*array.ListBuilder).ValueBuilder().(*array.StructBuilder)),
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

	b.builder.Append(true)
	if err := b.rb.Append(sm.Resource()); err != nil {
		return err
	}
	schemaUrl := sm.SchemaUrl()
	if schemaUrl == "" {
		b.schb.AppendNull()
	} else {
		if err := b.schb.AppendString(schemaUrl); err != nil {
			return err
		}
	}
	smetrics := sm.ScopeMetrics()
	sc := smetrics.Len()
	if sc > 0 {
		b.spsb.Append(true)
		b.spsb.Reserve(sc)
		for i := 0; i < sc; i++ {
			if err := b.smb.Append(smetrics.At(i)); err != nil {
				return err
			}
		}
	} else {
		b.spsb.Append(false)
	}
	return nil
}

// Release releases the memory allocated by the builder.
func (b *ResourceMetricsBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}
