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
	"fmt"

	"github.com/apache/arrow/go/v11/arrow"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	arrowutils "github.com/f5/otel-arrow-adapter/pkg/arrow2"
	otlp "github.com/f5/otel-arrow-adapter/pkg/otel/common/otlp2"
	"github.com/f5/otel-arrow-adapter/pkg/otel/constants"
)

type SpansIds struct {
	Id                  int
	TraceId             int
	SpanId              int
	TraceState          int
	ParentSpanId        int
	Name                int
	Kind                int
	StartTimeUnixNano   int
	EndTimeUnixNano     int
	Attributes          *otlp.AttributeIds
	DropAttributesCount int
	Events              *EventIds
	DropEventsCount     int
	Links               *LinkIds
	DropLinksCount      int
	Status              *StatusIds
}

type StatusIds struct {
	Id      int
	Code    int
	Message int
}

func NewSpansIds(scopeSpansDT *arrow.StructType) (*SpansIds, error) {
	id, spanDT, err := arrowutils.ListOfStructsFieldIDFromStruct(scopeSpansDT, constants.Spans)
	if err != nil {
		return nil, err
	}

	traceId, _ := arrowutils.FieldIDFromStruct(spanDT, constants.TraceId)
	spanId, _ := arrowutils.FieldIDFromStruct(spanDT, constants.SpanId)
	traceState, _ := arrowutils.FieldIDFromStruct(spanDT, constants.TraceState)
	parentSpanId, _ := arrowutils.FieldIDFromStruct(spanDT, constants.ParentSpanId)
	name, _ := arrowutils.FieldIDFromStruct(spanDT, constants.Name)
	kind, _ := arrowutils.FieldIDFromStruct(spanDT, constants.KIND)
	startTimeUnixNano, _ := arrowutils.FieldIDFromStruct(spanDT, constants.StartTimeUnixNano)
	endTimeUnixNano, _ := arrowutils.FieldIDFromStruct(spanDT, constants.EndTimeUnixNano)
	attributes, err := otlp.NewAttributeIds(spanDT)
	if err != nil {
		return nil, err
	}
	droppedAttributesCount, _ := arrowutils.FieldIDFromStruct(spanDT, constants.DroppedAttributesCount)
	events, err := NewEventIds(spanDT)
	if err != nil {
		return nil, err
	}

	droppedEventsCount, _ := arrowutils.FieldIDFromStruct(spanDT, constants.DroppedEventsCount)
	links, err := NewLinkIds(spanDT)
	if err != nil {
		return nil, err
	}

	droppedLinksCount, _ := arrowutils.FieldIDFromStruct(spanDT, constants.DroppedLinksCount)

	status, err := NewStatusIds(spanDT)
	if err != nil {
		return nil, err
	}

	return &SpansIds{
		Id:                  id,
		TraceId:             traceId,
		SpanId:              spanId,
		TraceState:          traceState,
		ParentSpanId:        parentSpanId,
		Name:                name,
		Kind:                kind,
		StartTimeUnixNano:   startTimeUnixNano,
		EndTimeUnixNano:     endTimeUnixNano,
		Attributes:          attributes,
		DropAttributesCount: droppedAttributesCount,
		Events:              events,
		DropEventsCount:     droppedEventsCount,
		Links:               links,
		DropLinksCount:      droppedLinksCount,
		Status:              status,
	}, nil
}

func NewStatusIds(spansDT *arrow.StructType) (*StatusIds, error) {
	statusId, statusDT, err := arrowutils.StructFieldIDFromStruct(spansDT, constants.Status)
	if err != nil {
		return nil, err
	}

	code, _ := arrowutils.FieldIDFromStruct(statusDT, constants.StatusCode)
	message, _ := arrowutils.FieldIDFromStruct(statusDT, constants.StatusMessage)

	return &StatusIds{
		Id:      statusId,
		Code:    code,
		Message: message,
	}, nil
}

func AppendSpanInto(spans ptrace.SpanSlice, los *arrowutils.ListOfStructs, row int, ids *SpansIds) error {
	span := spans.AppendEmpty()
	traceId, err := los.FixedSizeBinaryFieldByID(ids.TraceId, row)
	if err != nil {
		return err
	}
	if len(traceId) != 16 {
		return fmt.Errorf("trace_id field should be 16 bytes")
	}
	spanId, err := los.FixedSizeBinaryFieldByID(ids.SpanId, row)
	if err != nil {
		return err
	}
	if len(spanId) != 8 {
		return fmt.Errorf("span_id field should be 8 bytes")
	}
	traceState, err := los.StringFieldByID(ids.TraceState, row)
	if err != nil {
		return err
	}
	parentSpanId, err := los.FixedSizeBinaryFieldByID(ids.ParentSpanId, row)
	if err != nil {
		return err
	}
	if parentSpanId != nil && len(parentSpanId) != 8 {
		return fmt.Errorf("parent_span_id field should be 8 bytes")
	}
	name, err := los.StringFieldByID(ids.Name, row)
	if err != nil {
		return err
	}
	kind, err := los.I32FieldByID(ids.Kind, row)
	if err != nil {
		return err
	}
	startTimeUnixNano, err := los.TimestampFieldByID(ids.StartTimeUnixNano, row)
	if err != nil {
		return err
	}
	endTimeUnixNano, err := los.TimestampFieldByID(ids.EndTimeUnixNano, row)
	if err != nil {
		return err
	}
	droppedAttributesCount, err := los.U32FieldByID(ids.DropAttributesCount, row)
	if err != nil {
		return err
	}
	droppedEventsCount, err := los.U32FieldByID(ids.DropEventsCount, row)
	if err != nil {
		return err
	}
	droppedLinksCount, err := los.U32FieldByID(ids.DropLinksCount, row)
	if err != nil {
		return err
	}
	statusDt, statusArr, err := los.StructByID(ids.Status.Id, row)
	if err != nil {
		return err
	}
	if statusDt != nil {
		// Status exists
		message, err := arrowutils.StringFromStruct(statusArr, row, ids.Status.Message)
		if err != nil {
			return err
		}
		span.Status().SetMessage(message)

		code, err := arrowutils.I32FromStruct(statusArr, row, ids.Status.Code)
		if err != nil {
			return err
		}
		span.Status().SetCode(ptrace.StatusCode(code))
	}
	err = otlp.AppendAttributesInto(span.Attributes(), los.Array(), row, ids.Attributes)
	if err != nil {
		return err
	}

	if err := AppendEventsInto(span.Events(), los, row, ids.Events); err != nil {
		return err
	}
	if err := AppendLinksInto(span.Links(), los, row, ids.Links); err != nil {
		return err
	}
	var tid pcommon.TraceID
	var sid pcommon.SpanID
	var psid pcommon.SpanID
	copy(tid[:], traceId)
	copy(sid[:], spanId)
	copy(psid[:], parentSpanId)

	span.SetTraceID(tid)
	span.SetSpanID(sid)
	span.TraceState().FromRaw(traceState)
	span.SetParentSpanID(psid)
	span.SetName(name)
	span.SetKind(ptrace.SpanKind(kind))
	span.SetStartTimestamp(pcommon.Timestamp(startTimeUnixNano))
	span.SetEndTimestamp(pcommon.Timestamp(endTimeUnixNano))
	span.SetDroppedAttributesCount(droppedAttributesCount)
	span.SetDroppedEventsCount(droppedEventsCount)
	span.SetDroppedLinksCount(droppedLinksCount)
	return nil
}
