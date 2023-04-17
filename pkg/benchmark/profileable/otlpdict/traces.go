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

package otlpdict

import (
	"bytes"
	"encoding/binary"
	"io"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	otlpdicttraces "github.com/f5/otel-arrow-adapter/otlpdict/collector/trace/v1"
	otlpdictcommon "github.com/f5/otel-arrow-adapter/otlpdict/common/v1"
	otlpdicttrace "github.com/f5/otel-arrow-adapter/otlpdict/trace/v1"
	"github.com/f5/otel-arrow-adapter/pkg/benchmark"
	"github.com/f5/otel-arrow-adapter/pkg/benchmark/dataset"
)

type TracesProfileable struct {
	compression benchmark.CompressionAlgorithm

	// Input data.
	dataset dataset.TraceDataset

	// Next batch to encode. The result goes to nextBatchToSerialize.
	nextBatchToEncode []ptrace.Traces

	// Next batch to serialize. The result goes to byte buffers.
	nextBatchToSerialize []*otlpdicttraces.ExportTraceServiceRequest

	// Keep all sent traces for verification after delivery.
	allSentTraces [][]ptrace.Traces

	// Counts the number of traces received. Indexes into allSentTraces so that we can
	// compare sent against received.
	rcvTraceIdx int

	// Unary or streaming mode.
	unaryRpcMode bool

	// Sender's dictionaries.
	sKeyDict       sendDict
	sSpanNameDict  sendDict
	sEventNameDict sendDict
	sValDict       sendDict

	// Receiver's cumulative dictionaries.
	rKeyDict       []string
	rSpanNameDict  []string
	rEventNameDict []string
	rValDict       []string

	// A flag to compare sent and received data.
	verifyDelivery bool

	// Stores deserialized data that needs to be decoded.
	rcvTraces []otlpdicttraces.ExportTraceServiceRequest
}

// A pair of dictionaries used by sender.
type sendDict struct {
	// Cumulative dictionary. For unary is reset before each batch. For streaming mode is only reset at startup.
	cum map[string]uint64
	// Delta dictionary. Is reset before each batch.
	delta map[string]uint64
}

func NewTraceProfileable(dataset dataset.TraceDataset, compression benchmark.CompressionAlgorithm) *TracesProfileable {
	return &TracesProfileable{dataset: dataset, compression: compression, unaryRpcMode: false, verifyDelivery: true}
}

func (s *TracesProfileable) Name() string {
	return "OTLP DICT"
}

func (s *TracesProfileable) Tags() []string {
	modeStr := "unary rpc"
	if !s.unaryRpcMode {
		modeStr = "stream mode"
	}
	return []string{s.compression.String(), modeStr}
}

func (s *TracesProfileable) DatasetSize() int { return s.dataset.Len() }

func (s *TracesProfileable) CompressionAlgorithm() benchmark.CompressionAlgorithm {
	return s.compression
}

func (s *TracesProfileable) StartProfiling(io.Writer) {
	s.resetCumulativeDicts()
}

func (s *TracesProfileable) EndProfiling(io.Writer) {}

func (s *TracesProfileable) InitBatchSize(_ io.Writer, _ int) {}

func (s *TracesProfileable) PrepareBatch(_ io.Writer, startAt, size int) {
	s.nextBatchToEncode = s.dataset.Traces(startAt, size)
	s.allSentTraces = append(s.allSentTraces, s.nextBatchToEncode)
}

func dictionizeStr(dict sendDict, str *string, ref *uint64) {
	var idx uint64
	var ok bool
	if idx, ok = dict.cum[*str]; !ok {
		idx = uint64(len(dict.cum))

		buf := make([]byte, 10)
		n := binary.PutUvarint(buf, idx)
		if n >= len(*str) {
			// Not worth using ref.
			return
		}

		dict.cum[*str] = idx
		dict.delta[*str] = idx
	}
	*str = ""
	*ref = idx
}

func dictionizeAttrs(keyDict, valDict sendDict, attrs []*otlpdictcommon.KeyValue) {
	for _, attr := range attrs {
		dictionizeStr(keyDict, &attr.Key, &attr.KeyRef)

		switch v := attr.Value.Value.(type) {
		case *otlpdictcommon.AnyValue_StringValue:
			str := v.StringValue
			var idx uint64
			var ok bool
			if idx, ok = valDict.cum[str]; !ok {
				idx = uint64(len(valDict.cum))
				buf := make([]byte, 10)
				n := binary.PutUvarint(buf, idx)
				if n >= len(str) {
					// Not worth using ref.
					break
				}

				valDict.cum[str] = idx
				valDict.delta[str] = idx
			}
			attr.Value.Value = &otlpdictcommon.AnyValue_StringRef{StringRef: idx}
		default:
		}
	}
}

func (s *TracesProfileable) ConvertOtlpToOtlpArrow(_ io.Writer, _, _ int) {
	// In the standard OTLP exporter the incoming messages are already OTLP messages,
	// so we don't need to create or convert them.

	s.sKeyDict.delta = map[string]uint64{}
	s.sValDict.delta = map[string]uint64{}
	s.sEventNameDict.delta = map[string]uint64{}
	s.sSpanNameDict.delta = map[string]uint64{}

	if s.unaryRpcMode {
		s.resetCumulativeDicts()
	}

	s.nextBatchToSerialize = nil
	for _, traceReq := range s.nextBatchToEncode {
		// Make a copy of traceReq. In production implementation this is will be unnecessary
		// but we are forced to do it here since the profiler's source data is in the old OTLP format.

		// First marshal it to bytes.
		r := ptraceotlp.NewExportRequestFromTraces(traceReq)
		bytes, err := r.MarshalProto()
		if err != nil {
			panic(err)
		}
		// Then unmarshal from bytes to OTLP DICT's Protobuf message.
		destTrace := &otlpdicttraces.ExportTraceServiceRequest{}
		err = proto.Unmarshal(bytes, destTrace)
		if err != nil {
			panic(err)
		}

		s.nextBatchToSerialize = append(s.nextBatchToSerialize, destTrace)

		// Now do the actual dictionary encoding. This is the part that needs to be benchmarked
		// separately in the future.
		rss := destTrace.ResourceSpans
		for i := 0; i < len(rss); i++ {
			rs := rss[i]

			dictionizeAttrs(s.sKeyDict, s.sValDict, rs.Resource.Attributes)

			ss := rs.ScopeSpans
			for j := 0; j < len(ss); j++ {
				sps := ss[j].Spans
				for k := 0; k < len(sps); k++ {
					span := sps[k]
					dictionizeStr(s.sSpanNameDict, &span.Name, &span.NameRef)
					dictionizeAttrs(s.sKeyDict, s.sValDict, span.Attributes)

					for m := 0; m < len(span.Events); m++ {
						e := span.Events[m]
						dictionizeStr(s.sEventNameDict, &e.Name, &e.NameRef)
						dictionizeAttrs(s.sKeyDict, s.sValDict, e.Attributes)
					}
					for m := 0; m < len(span.Links); m++ {
						l := span.Links[m]
						dictionizeAttrs(s.sKeyDict, s.sValDict, l.Attributes)
					}
				}
			}
		}
	}

	// Include dictionaries in the first request of the batch.
	s.nextBatchToSerialize[0].ValDict = dictToProto(s.sValDict)
	s.nextBatchToSerialize[0].KeyDict = dictToProto(s.sKeyDict)
	s.nextBatchToSerialize[0].EventNameDict = dictToProto(s.sEventNameDict)
	s.nextBatchToSerialize[0].SpanNameDict = dictToProto(s.sSpanNameDict)
}

func (s *TracesProfileable) resetCumulativeDicts() {
	// Note: we don't use string with ref index 0, so we initialize maps on both ends to
	// avoid using the 0 index in the payload.

	s.sKeyDict.cum = map[string]uint64{"": 0}
	s.sSpanNameDict.cum = map[string]uint64{"": 0}
	s.sEventNameDict.cum = map[string]uint64{"": 0}
	s.sValDict.cum = map[string]uint64{"": 0}
	s.rKeyDict = []string{""}
	s.rValDict = []string{""}
	s.rSpanNameDict = []string{""}
	s.rEventNameDict = []string{""}
}

func dictToProto(dict sendDict) *otlpdictcommon.Dict {
	ofs := len(dict.cum) - len(dict.delta)
	dest := &otlpdictcommon.Dict{
		StartIndex: uint64(ofs),
		Values:     make([]string, len(dict.delta)),
	}

	for k, v := range dict.delta {
		dest.Values[int(v)-ofs] = k
	}

	return dest
}

func (s *TracesProfileable) Process(io.Writer) string {
	// Not used in this benchmark
	return ""
}

func (s *TracesProfileable) Serialize(io.Writer) ([][]byte, error) {
	buffers := make([][]byte, len(s.nextBatchToSerialize))
	for i, t := range s.nextBatchToSerialize {
		bytes, err := proto.Marshal(t)
		if err != nil {
			return nil, err
		}
		buffers[i] = bytes
	}

	return buffers, nil
}

func (s *TracesProfileable) Deserialize(_ io.Writer, buffers [][]byte) {
	s.rcvTraces = make([]otlpdicttraces.ExportTraceServiceRequest, len(buffers))

	for i, b := range buffers {
		if err := proto.Unmarshal(b, &s.rcvTraces[i]); err != nil {
			panic(err)
		}
	}
}

func (s *TracesProfileable) equalTraces(left ptrace.Traces, right otlpdicttraces.ExportTraceServiceRequest) bool {
	lrss := left.ResourceSpans()
	rrss := right.ResourceSpans
	if lrss.Len() != len(rrss) {
		return false
	}
	for i := 0; i < lrss.Len(); i++ {
		lrs := lrss.At(i)
		rrs := rrss[i]
		lr := lrs.Resource()
		rr := rrs.Resource
		if !s.equalAttrs(lr.Attributes(), rr.Attributes) {
			return false
		}
		lss := lrs.ScopeSpans()
		rss := rrs.ScopeSpans
		for j := 0; j < lss.Len(); j++ {
			ls := lss.At(j)
			rs := rss[j]
			if !s.equalScopes(ls.Scope(), rs.Scope) {
				return false
			}
			lsps := ls.Spans()
			rsps := rs.Spans
			for k := 0; k < lsps.Len(); k++ {
				lsp := lsps.At(k)
				rsp := rsps[k]
				if !s.equalSpans(lsp, rsp) {
					return false
				}
			}
		}
	}
	return true
}

func (s *TracesProfileable) equalScopes(
	left pcommon.InstrumentationScope, right *otlpdictcommon.InstrumentationScope,
) bool {
	if left.Name() != right.Name {
		return false
	}
	if left.Version() != right.Version {
		return false
	}
	if !s.equalAttrs(left.Attributes(), right.Attributes) {
		return false
	}
	if left.DroppedAttributesCount() != right.DroppedAttributesCount {
		return false
	}
	return true
}
func (s *TracesProfileable) equalSpans(left ptrace.Span, right *otlpdicttrace.Span) bool {
	ltid := left.TraceID()
	if !bytes.Equal(ltid[:], right.TraceId) {
		return false
	}
	lsid := left.SpanID()
	if !bytes.Equal(lsid[:], right.SpanId) {
		return false
	}
	lpsid := left.ParentSpanID()
	if !bytes.Equal(lpsid[:], right.ParentSpanId) {
		if !lpsid.IsEmpty() || right.ParentSpanId != nil {
			return false
		}
	}
	if left.TraceState().AsRaw() != right.TraceState {
		return false
	}
	if !s.equalAttrs(left.Attributes(), right.Attributes) {
		return false
	}

	rname := right.Name
	if right.NameRef != 0 {
		rname = s.rSpanNameDict[right.NameRef]
	}
	if left.Name() != rname {
		return false
	}

	if int32(left.Kind()) != int32(right.Kind) {
		return false
	}

	if uint64(left.StartTimestamp()) != right.StartTimeUnixNano {
		return false
	}
	if uint64(left.EndTimestamp()) != right.EndTimeUnixNano {
		return false
	}

	if left.DroppedAttributesCount() != right.DroppedAttributesCount {
		return false
	}

	if left.DroppedEventsCount() != right.DroppedEventsCount {
		return false
	}

	if left.DroppedLinksCount() != right.DroppedLinksCount {
		return false
	}

	if !s.equalEvents(left.Events(), right.Events) {
		return false
	}

	if !s.equalLinks(left.Links(), right.Links) {
		return false
	}

	if !s.equalStatus(left.Status(), right.Status) {
		return false
	}

	return true
}

func (s *TracesProfileable) equalAttrs(left pcommon.Map, right []*otlpdictcommon.KeyValue) bool {
	if len(right) != left.Len() {
		return false
	}

	i := 0
	left.Range(
		func(lkey string, lval pcommon.Value) bool {
			rkv := right[i]
			rkey := rkv.Key
			if rkv.KeyRef != 0 {
				rkey = s.rKeyDict[rkv.KeyRef]
			}
			if lkey != rkey {
				return false
			}
			rval := rkv.Value
			if !s.equalValues(lval, rval) {
				return false
			}

			i++
			return true
		},
	)
	return true
}

func (s *TracesProfileable) equalValues(left pcommon.Value, right *otlpdictcommon.AnyValue) bool {
	switch left.Type() {
	case pcommon.ValueTypeStr:
		if right.GetStringRef() != 0 {
			val := s.rValDict[right.GetStringRef()]
			return left.Str() == val
		}
		return left.Str() == right.GetStringValue()
	case pcommon.ValueTypeInt:
		return left.Int() == right.GetIntValue()
	case pcommon.ValueTypeBool:
		return left.Bool() == right.GetBoolValue()
	case pcommon.ValueTypeDouble:
		return left.Double() == right.GetDoubleValue()
	case pcommon.ValueTypeEmpty:
		return right.Value == nil
	case pcommon.ValueTypeBytes:
		return bytes.Equal(left.Bytes().AsRaw(), right.GetBytesValue())
	default:
		panic("not implemented")
	}

	return true
}

func deserializeDict(src *otlpdictcommon.Dict, dest *[]string) {
	if src == nil {
		return
	}
	if int(src.StartIndex) != len(*dest) {
		panic("invalid dict continuation")
	}
	for _, s := range src.Values {
		*dest = append(*dest, s)
	}
}

func (s *TracesProfileable) equalEvents(left ptrace.SpanEventSlice, right []*otlpdicttrace.Span_Event) bool {
	if left.Len() != len(right) {
		return false
	}
	for i := 0; i < left.Len(); i++ {
		if !s.equalEvent(left.At(i), right[i]) {
			return false
		}
	}
	return true
}

func (s *TracesProfileable) equalEvent(left ptrace.SpanEvent, right *otlpdicttrace.Span_Event) bool {
	if uint64(left.Timestamp()) != right.TimeUnixNano {
		return false
	}
	rname := right.Name
	if right.NameRef != 0 {
		rname = s.rEventNameDict[right.NameRef]
	}
	if left.Name() != rname {
		return false
	}
	if !s.equalAttrs(left.Attributes(), right.Attributes) {
		return false
	}
	if left.DroppedAttributesCount() != right.DroppedAttributesCount {
		return false
	}
	return true
}

func (s *TracesProfileable) equalLinks(left ptrace.SpanLinkSlice, right []*otlpdicttrace.Span_Link) bool {
	if left.Len() != len(right) {
		return false
	}
	for i := 0; i < left.Len(); i++ {
		if !s.equalLink(left.At(i), right[i]) {
			return false
		}
	}
	return true
}

func (s *TracesProfileable) equalLink(left ptrace.SpanLink, right *otlpdicttrace.Span_Link) bool {
	// TODO: implement this. Not needed for now since our test data does not have links.
	if !s.equalAttrs(left.Attributes(), right.Attributes) {
		return false
	}
	if left.DroppedAttributesCount() != right.DroppedAttributesCount {
		return false
	}
	return true
}

func (s *TracesProfileable) equalStatus(left ptrace.Status, right *otlpdicttrace.Status) bool {
	if uint32(left.Code()) != uint32(right.Code) {
		return false
	}
	if left.Message() != right.Message {
		return false
	}
	return true
}

func (s *TracesProfileable) ConvertOtlpArrowToOtlp(_ io.Writer) {
	for i := 0; i < len(s.rcvTraces); i++ {
		deserializeDict(s.rcvTraces[i].ValDict, &s.rValDict)
		deserializeDict(s.rcvTraces[i].KeyDict, &s.rKeyDict)
		deserializeDict(s.rcvTraces[i].SpanNameDict, &s.rSpanNameDict)
		deserializeDict(s.rcvTraces[i].EventNameDict, &s.rEventNameDict)

		// Compare received data to sent to make sure the protocol works correctly.
		// This should be disabled in speed benchmarks since it is not part of normal
		// protocol operation.
		if s.verifyDelivery {
			sentTraces := s.allSentTraces[s.rcvTraceIdx]
			s.rcvTraceIdx++
			for _, srcTrace := range sentTraces {
				if !s.equalTraces(srcTrace, s.rcvTraces[i]) {
					panic("sent and received traces are not equal")
				}
			}
		}
	}
}

func (s *TracesProfileable) Clear() {
	s.nextBatchToEncode = nil
}

func (s *TracesProfileable) ShowStats() {}

func (s *TracesProfileable) EnableUnaryRpcMode() {
	s.unaryRpcMode = true
}
