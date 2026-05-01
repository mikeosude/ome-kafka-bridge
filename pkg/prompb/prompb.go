// Package prompb contains a minimal hand-rolled implementation of the
// Prometheus remote_write protobuf types. This avoids pulling in the full
// prometheus/prometheus dependency tree just for the protobuf definitions.
//
// Wire format is compatible with prometheus/prometheus prompb.WriteRequest.
package prompb

import (
	"encoding/binary"
	"fmt"
	"math"
)

// Label is a name/value pair.
type Label struct {
	Name  string
	Value string
}

// Sample is a single metric data point.
type Sample struct {
	Value     float64
	Timestamp int64 // milliseconds
}

// TimeSeries is a set of labels with one or more samples.
type TimeSeries struct {
	Labels  []Label
	Samples []Sample
}

// WriteRequest is the top-level remote_write payload.
type WriteRequest struct {
	Timeseries []TimeSeries
}

// Marshal encodes a WriteRequest into the Prometheus protobuf wire format.
// Field numbers match the official prometheus/prometheus prompb definitions:
//   WriteRequest:   field 1 = timeseries (repeated)
//   TimeSeries:     field 1 = labels (repeated), field 2 = samples (repeated)
//   Label:          field 1 = name (string),     field 2 = value (string)
//   Sample:         field 1 = value (double),    field 2 = timestamp (int64)
func (r *WriteRequest) Marshal() ([]byte, error) {
	var buf []byte
	for _, ts := range r.Timeseries {
		tsBytes, err := marshalTimeSeries(ts)
		if err != nil {
			return nil, err
		}
		// field 1, wire type 2 (length-delimited)
		buf = appendTag(buf, 1, 2)
		buf = appendVarint(buf, uint64(len(tsBytes)))
		buf = append(buf, tsBytes...)
	}
	return buf, nil
}

func marshalTimeSeries(ts TimeSeries) ([]byte, error) {
	var buf []byte
	for _, lbl := range ts.Labels {
		lblBytes, err := marshalLabel(lbl)
		if err != nil {
			return nil, err
		}
		buf = appendTag(buf, 1, 2)
		buf = appendVarint(buf, uint64(len(lblBytes)))
		buf = append(buf, lblBytes...)
	}
	for _, s := range ts.Samples {
		sBytes, err := marshalSample(s)
		if err != nil {
			return nil, err
		}
		buf = appendTag(buf, 2, 2)
		buf = appendVarint(buf, uint64(len(sBytes)))
		buf = append(buf, sBytes...)
	}
	return buf, nil
}

func marshalLabel(l Label) ([]byte, error) {
	if l.Name == "" {
		return nil, fmt.Errorf("label name must not be empty")
	}
	var buf []byte
	buf = appendTag(buf, 1, 2)
	buf = appendString(buf, l.Name)
	buf = appendTag(buf, 2, 2)
	buf = appendString(buf, l.Value)
	return buf, nil
}

func marshalSample(s Sample) ([]byte, error) {
	var buf []byte
	// field 1: double (wire type 1 = 64-bit)
	buf = appendTag(buf, 1, 1)
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, math.Float64bits(s.Value))
	buf = append(buf, b...)
	// field 2: int64 (wire type 0 = varint), encoded as zigzag for signed
	buf = appendTag(buf, 2, 0)
	buf = appendVarint(buf, uint64(s.Timestamp))
	return buf, nil
}

func appendTag(buf []byte, fieldNum, wireType uint64) []byte {
	return appendVarint(buf, (fieldNum<<3)|wireType)
}

func appendVarint(buf []byte, v uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(buf, tmp[:n]...)
}

func appendString(buf []byte, s string) []byte {
	buf = appendVarint(buf, uint64(len(s)))
	return append(buf, s...)
}
