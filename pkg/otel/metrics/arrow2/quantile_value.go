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

	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema/builder"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

// QuantileValueDT is the Arrow Data Type describing a quantile value.
var (
	QuantileValueDT = arrow.StructOf(
		arrow.Field{Name: constants.SummaryQuantile, Type: arrow.PrimitiveTypes.Float64, Metadata: schema.Metadata(schema.Optional)},
		arrow.Field{Name: constants.SummaryValue, Type: arrow.PrimitiveTypes.Float64, Metadata: schema.Metadata(schema.Optional)},
	)
)

// QuantileValueBuilder is a builder for a quantile value.
type QuantileValueBuilder struct {
	released bool

	builder *builder.StructBuilder

	sqb *builder.Float64Builder // summary quantile builder
	svb *builder.Float64Builder // summary quantile value builder
}

// QuantileValueBuilderFrom creates a new QuantileValueBuilder from an existing StructBuilder.
func QuantileValueBuilderFrom(ndpb *builder.StructBuilder) *QuantileValueBuilder {
	return &QuantileValueBuilder{
		released: false,
		builder:  ndpb,

		sqb: ndpb.Float64Builder(constants.SummaryQuantile),
		svb: ndpb.Float64Builder(constants.SummaryValue),
	}
}

// Build builds the underlying array.
//
// Once the array is no longer needed, Release() should be called to free the memory.
func (b *QuantileValueBuilder) Build() (*array.Struct, error) {
	if b.released {
		return nil, fmt.Errorf("QuantileValueBuilder: Build() called after Release()")
	}

	defer b.Release()
	return b.builder.NewStructArray(), nil
}

// Release releases the underlying memory.
func (b *QuantileValueBuilder) Release() {
	if b.released {
		return
	}

	b.released = true
	b.builder.Release()
}

// Append appends a new quantile value to the builder.
func (b *QuantileValueBuilder) Append(sdp pmetric.SummaryDataPointValueAtQuantile) error {
	if b.released {
		return fmt.Errorf("QuantileValueBuilder: Reserve() called after Release()")
	}

	return b.builder.Append(sdp, func() error {
		b.sqb.Append(sdp.Quantile())
		b.svb.Append(sdp.Value())
		return nil
	})
}
