// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tracing

import (
	"context"
	"fmt"
	"runtime/trace"

	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
)

// TiDBTrace is set as Baggage on traces which are used for tidb tracing.
const TiDBTrace = "tr"

// A CallbackRecorder immediately invokes itself on received trace spans.
type CallbackRecorder func(sp basictracer.RawSpan)

// RecordSpan implements basictracer.SpanRecorder.
func (cr CallbackRecorder) RecordSpan(sp basictracer.RawSpan) {
	cr(sp)
}

// Span is a wrapper of opentracing.Span and trace.Region.
type Span struct {
	span   opentracing.Span
	region *trace.Region
}

// LogKV wraps the (opentracing.Span).LogKV.
func (span *Span) LogKV(alternatingKeyValues ...interface{}) {
	if span.span != nil {
		span.span.LogKV(alternatingKeyValues...)
	}
}

// Finish wraps the (opentracing.Span).Finish and (*trace.Region).End.
func (span *Span) Finish() {
	if span.span != nil {
		span.span.Finish()
	}
	if span.region != nil {
		span.region.End()
	}
}

// NewRecordedTrace returns a Span which records directly via the specified
// callback.
func NewRecordedTrace(opName string, callback func(sp basictracer.RawSpan)) opentracing.Span {
	tr := basictracer.New(CallbackRecorder(callback))
	opentracing.SetGlobalTracer(tr)
	sp := tr.StartSpan(opName)
	sp.SetBaggageItem(TiDBTrace, "1")
	return sp
}

// SpanFromContext returns the span obtained from the context or, if none is found, a new one started through tracer.
func SpanFromContext(ctx context.Context) (sp opentracing.Span) {
	if sp = opentracing.SpanFromContext(ctx); sp == nil {
		return nil
	}
	return sp
}

// ChildSpanFromContext return a non-nil span. If span can be got from ctx, then returned span is
// a child of such span. Otherwise, returned span is a noop span.
func ChildSpanFromContext(ctx context.Context, opName string) (Span, context.Context) {
	var region *trace.Region
	if trace.IsEnabled() {
		region = trace.StartRegion(ctx, opName)
	}
	if sp := opentracing.SpanFromContext(ctx); sp != nil && sp.Tracer() != nil {
		child := opentracing.StartSpan(opName, opentracing.ChildOf(sp.Context()))
		return Span{child, region}, opentracing.ContextWithSpan(ctx, child)
	}
	return Span{nil, region}, ctx
}

// ChildSpanFromContextFmt return a non-nil span. If span can be got from ctx, then returned span is
// a child of such span. Otherwise, returned span is a noop span.
func ChildSpanFromContextFmt(ctx context.Context, format string, args ...interface{}) (Span, context.Context) {
	if sp := opentracing.SpanFromContext(ctx); (sp == nil || sp.Tracer() == nil) && !trace.IsEnabled() {
		return Span{}, ctx
	}
	return ChildSpanFromContext(ctx, fmt.Sprintf(format, args...))
}
