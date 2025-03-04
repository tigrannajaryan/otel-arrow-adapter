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

package arrow_record

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/f5/otel-arrow-adapter/pkg/otel/assert"
)

// TestMetricsWithNoDictionary
// Initial dictionary index size is 0 ==> no dictionary.
func TestMetricsWithNoDictionary(t *testing.T) {
	t.Parallel()

	//pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	//defer pool.AssertSize(t, 0)
	pool := memory.NewGoAllocator()

	producer := NewProducerWithOptions(
		WithAllocator(pool),
		WithNoDictionary(),
	)
	defer func() {
		if err := producer.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	consumer := NewConsumer()
	defer func() {
		if err := consumer.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	for i := 0; i < 300; i++ {
		metrics := GenerateMetrics(0, math.MaxUint8+1)
		batch, err := producer.BatchArrowRecordsFromMetrics(metrics)
		require.NoError(t, err)
		require.NotNil(t, batch)

		received, err := consumer.MetricsFrom(batch)
		require.NoError(t, err)
		require.Equal(t, 1, len(received))

		assert.Equiv(
			t,
			[]json.Marshaler{pmetricotlp.NewExportRequestFromMetrics(metrics)},
			[]json.Marshaler{pmetricotlp.NewExportRequestFromMetrics(received[0])},
		)
	}

	builder := producer.MetricsRecordBuilderExt()
	require.Equal(t, 0, len(builder.Events().DictionariesWithOverflow))
}

// TestMetricsSingleBatchWithDictionaryOverflow
// Initial dictionary size uint8.
// First batch of uint8 + 1 spans ==> dictionary overflow on 3 fields.
// Other consecutive batches should not trigger any other dictionary overflow.
func TestMetricsSingleBatchWithDictionaryOverflow(t *testing.T) {
	t.Parallel()

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	producer := NewProducerWithOptions(
		WithAllocator(pool),
		WithUint8InitDictIndex(),
		WithUint32LimitDictIndex(),
	)
	defer func() {
		if err := producer.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	consumer := NewConsumer()
	defer func() {
		if err := consumer.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	for i := 0; i < 10; i++ {
		metrics := GenerateMetrics(0, math.MaxUint8+1)
		batch, err := producer.BatchArrowRecordsFromMetrics(metrics)
		require.NoError(t, err)
		require.NotNil(t, batch)

		received, err := consumer.MetricsFrom(batch)
		require.NoError(t, err)
		require.Equal(t, 1, len(received))

		assert.Equiv(
			t,
			[]json.Marshaler{pmetricotlp.NewExportRequestFromMetrics(metrics)},
			[]json.Marshaler{pmetricotlp.NewExportRequestFromMetrics(received[0])},
		)
	}

	builder := producer.MetricsRecordBuilderExt()
	dictionariesIndexTypeChanged := builder.Events().DictionariesIndexTypeChanged
	require.Equal(t, 2, len(dictionariesIndexTypeChanged))
	require.Equal(t, "uint16", dictionariesIndexTypeChanged["resource_metrics.item.scope_metrics.item.univariate_metrics.name"])
	require.Equal(t, "uint16", dictionariesIndexTypeChanged["resource_metrics.item.scope_metrics.item.univariate_metrics.description"])
}

// TestMetricsMultiBatchWithDictionaryOverflow
// Initial dictionary size uint8.
// First and second batch of uint8/2 spans (each) ==> no dictionary overflow.
// Third batch should trigger dictionary overflow.
// All other consecutive batches should not trigger any other dictionary overflow.
func TestMetricsMultiBatchWithDictionaryOverflow(t *testing.T) {
	t.Parallel()

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	producer := NewProducerWithOptions(
		WithAllocator(pool),
		WithUint8InitDictIndex(),
		WithUint32LimitDictIndex(),
	)
	defer func() {
		if err := producer.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	consumer := NewConsumer()
	defer func() {
		if err := consumer.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	for i := 0; i < 10; i++ {
		metrics := GenerateMetrics(i*((math.MaxUint8/2)+1), (math.MaxUint8/2)+1)
		batch, err := producer.BatchArrowRecordsFromMetrics(metrics)
		require.NoError(t, err)
		require.NotNil(t, batch)

		received, err := consumer.MetricsFrom(batch)
		require.NoError(t, err)
		require.Equal(t, 1, len(received))

		assert.Equiv(
			t,
			[]json.Marshaler{pmetricotlp.NewExportRequestFromMetrics(metrics)},
			[]json.Marshaler{pmetricotlp.NewExportRequestFromMetrics(received[0])},
		)
	}

	builder := producer.MetricsRecordBuilderExt()
	dictionariesIndexTypeChanged := builder.Events().DictionariesIndexTypeChanged
	require.Equal(t, 2, len(dictionariesIndexTypeChanged))
	require.Equal(t, "uint16", dictionariesIndexTypeChanged["resource_metrics.item.scope_metrics.item.univariate_metrics.name"])
	require.Equal(t, "uint16", dictionariesIndexTypeChanged["resource_metrics.item.scope_metrics.item.univariate_metrics.description"])
}

// TestMetricsSingleBatchWithDictionaryLimit
// Initial dictionary size uint8.
// Limit dictionary index size is uint8.
// First batch of uint8 + 1 spans ==> dictionary index type limit reached so fallback to utf8 or binary.
func TestMetricsSingleBatchWithDictionaryLimit(t *testing.T) {
	t.Parallel()

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	producer := NewProducerWithOptions(
		WithAllocator(pool),
		WithUint8InitDictIndex(),
		WithUint8LimitDictIndex(),
	)
	defer func() {
		if err := producer.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	consumer := NewConsumer()
	defer func() {
		if err := consumer.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	for i := 0; i < 10; i++ {
		metrics := GenerateMetrics(0, math.MaxUint8+1)
		batch, err := producer.BatchArrowRecordsFromMetrics(metrics)
		require.NoError(t, err)
		require.NotNil(t, batch)

		received, err := consumer.MetricsFrom(batch)
		require.NoError(t, err)
		require.Equal(t, 1, len(received))

		assert.Equiv(
			t,
			[]json.Marshaler{pmetricotlp.NewExportRequestFromMetrics(metrics)},
			[]json.Marshaler{pmetricotlp.NewExportRequestFromMetrics(received[0])},
		)
	}

	builder := producer.MetricsRecordBuilderExt()
	dictionaryWithOverflow := builder.Events().DictionariesWithOverflow
	require.Equal(t, 2, len(dictionaryWithOverflow))
	require.Equal(t, "utf8", dictionaryWithOverflow["resource_metrics.item.scope_metrics.item.univariate_metrics.name"])
	require.Equal(t, "utf8", dictionaryWithOverflow["resource_metrics.item.scope_metrics.item.univariate_metrics.description"])
}

func GenerateMetrics(initValue int, metricCount int) pmetric.Metrics {
	metrics := pmetric.NewMetrics()

	rms := metrics.ResourceMetrics()
	rms.EnsureCapacity(1)

	rm := rms.AppendEmpty()
	rm.SetSchemaUrl("schema")

	sms := rm.ScopeMetrics()
	sms.EnsureCapacity(1)

	metricSet := sms.AppendEmpty().Metrics()
	metricSet.EnsureCapacity(metricCount)

	for i := 0; i < metricCount; i++ {
		metric := metricSet.AppendEmpty()
		metric.SetName(fmt.Sprintf("metric_%d", initValue+i))
		metric.SetDescription(fmt.Sprintf("metric_%d_description", initValue+i))
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints()
		dp.EnsureCapacity(1)
		dp.AppendEmpty().SetIntValue(int64(i))
	}

	return metrics
}
