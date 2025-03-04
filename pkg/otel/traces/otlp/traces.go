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

package otlp

import (
	"github.com/apache/arrow/go/v11/arrow"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type TraceIds struct {
	ResourceSpans *ResourceSpansIds
}

// TracesFrom creates a [ptrace.Traces] from the given Arrow Record.
func TracesFrom(record arrow.Record) (ptrace.Traces, error) {
	traces := ptrace.NewTraces()

	traceIds, err := SchemaToIds(record.Schema())
	if err != nil {
		return traces, err
	}

	resSpansSlice := traces.ResourceSpans()
	resSpansCount := int(record.NumRows())
	resSpansSlice.EnsureCapacity(resSpansCount)

	// TODO there is probably two nested lists that could be replaced by a single list (traces, resource spans). This could simplify a future query layer.

	err = AppendResourceSpansInto(traces, record, traceIds)
	return traces, err
}

func SchemaToIds(schema *arrow.Schema) (*TraceIds, error) {
	resSpansIds, err := NewResourceSpansIds(schema)
	if err != nil {
		return nil, err
	}
	return &TraceIds{
		ResourceSpans: resSpansIds,
	}, nil
}
