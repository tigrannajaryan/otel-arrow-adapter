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
	"go.opentelemetry.io/collector/pdata/plog"
)

type LogsIds struct {
	ResourceLogs *ResourceLogsIds
}

// LogsFrom creates a [plog.Logs] from the given Arrow Record.
func LogsFrom(record arrow.Record) (plog.Logs, error) {
	logs := plog.NewLogs()

	ids, err := SchemaToIds(record.Schema())
	if err != nil {
		return logs, err
	}

	// TODO there is probably two nested lists that could be replaced by a single list (traces, resource spans). This could simplify a future query layer.

	err = AppendResourceLogsInto(logs, record, ids)
	return logs, err
}

func SchemaToIds(schema *arrow.Schema) (*LogsIds, error) {
	resLogsIds, err := NewResourceLogsIds(schema)
	if err != nil {
		return nil, err
	}
	return &LogsIds{
		ResourceLogs: resLogsIds,
	}, nil
}
