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

package logs // import "github.com/f5/otel-arrow-adapter/collector/gen/receiver/otlpreceiver/internal/logs"

import (
	"context"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/obsreport"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
)

const (
	dataFormatProtobuf = "protobuf"
)

// Receiver is the type used to handle spans from OpenTelemetry exporters.
type Receiver struct {
	nextConsumer consumer.Logs
	obsrecv      *obsreport.Receiver
}

// New creates a new Receiver reference.
func New(nextConsumer consumer.Logs, obsrecv *obsreport.Receiver) *Receiver {
	return &Receiver{
		nextConsumer: nextConsumer,
		obsrecv:      obsrecv,
	}
}

// Export implements the service Export logs func.
func (r *Receiver) Export(ctx context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	ld := req.Logs()
	numSpans := ld.LogRecordCount()
	if numSpans == 0 {
		return plogotlp.NewExportResponse(), nil
	}

	ctx = r.obsrecv.StartLogsOp(ctx)
	err := r.nextConsumer.ConsumeLogs(ctx, ld)
	r.obsrecv.EndLogsOp(ctx, dataFormatProtobuf, numSpans, err)

	return plogotlp.NewExportResponse(), err
}

func (r *Receiver) Consumer() consumer.Logs {
	return r.nextConsumer
}
