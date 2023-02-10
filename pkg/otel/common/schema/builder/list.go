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

package builder

import (
	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"

	"github.com/f5/otel-arrow-adapter/pkg/otel/common/schema"
)

// ListBuilder is a wrapper around the arrow.ListBuilder.
type ListBuilder struct {
	protoDataType *arrow.ListType
	builder       *array.ListBuilder
	transformNode *schema.TransformNode
	updateRequest *SchemaUpdateRequest
}

func (lb *ListBuilder) valueProtoDataTypeAndTransformNode() (arrow.DataType, *schema.TransformNode) {
	return lb.protoDataType.Elem(), lb.transformNode.Children[0]
}

func (lb *ListBuilder) valueBuilder() array.Builder {
	if lb.builder != nil {
		return lb.builder.ValueBuilder()
	} else {
		return nil
	}
}

func (lb *ListBuilder) Append(itemCount int, itemsAppender func()) {
	if itemCount == 0 {
		lb.AppendNull()
		return
	}

	lb.Reserve(itemCount)
	itemsAppender()
}

// AppendNull adds a null list to the underlying builder. If the builder is
// nil we do nothing as we have no information about the presence of this field
// in the data.
func (lb *ListBuilder) AppendNull() {
	if lb.builder != nil {
		lb.builder.AppendNull()
		return
	}
}

// Reserve adds a list to the underlying builder or updates the transform node
// if the builder is nil (and if the append parameter is true).
// The list is initialized with the given number of items.
func (lb *ListBuilder) Reserve(numItems int) {
	if lb.builder != nil {
		lb.builder.Append(numItems > 0)
		lb.builder.Reserve(numItems)
		return
	}

	// If the builder is nil, then the transform node is not optional.
	if numItems > 0 {
		lb.transformNode.RemoveOptional()
		lb.updateRequest.count++
	}
}

// SparseUnionBuilder returns a SparseUnionBuilder initialized with the
// underlying arrow array builder representing a list of elements of
// sparse union type.
func (lb *ListBuilder) SparseUnionBuilder() *SparseUnionBuilder {
	builder := lb.valueBuilder()
	protoDataType, transformNode := lb.valueProtoDataTypeAndTransformNode()

	if builder != nil {
		return &SparseUnionBuilder{protoDataType: protoDataType.(*arrow.SparseUnionType), builder: builder.(*array.SparseUnionBuilder), transformNode: transformNode, updateRequest: lb.updateRequest}
	} else {
		return &SparseUnionBuilder{protoDataType: protoDataType.(*arrow.SparseUnionType), builder: nil, transformNode: transformNode, updateRequest: lb.updateRequest}
	}
}
