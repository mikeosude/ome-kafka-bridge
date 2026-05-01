// Package broker implements a minimal Kafka broker sufficient for Dell
// OpenManage Enterprise to connect to as a producer.
//
// OME uses librdkafka which negotiates ApiVersions v3 (flexible/compact encoding).
// This implementation handles:
//   - ApiVersions v0-v3 (including flexible encoding)
//   - Metadata v0-v9
//   - Produce v0-v9
//   - SaslHandshake / SaslAuthenticate (enough to reject gracefully)
//
// Flexible encoding (KIP-482): array lengths use unsigned varints (N+1),
// strings use unsigned varint length prefix (N+1), and each struct has a
// trailing tagged-fields section (0x00 = empty).
package broker

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	cfg "github.com/mikeosude/ome-kafka-bridge/internal/config"
)

// Message is a fully decoded Kafka ProduceRequest record.
type Message struct {
	Topic     string
	Partition int32
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

// Broker is the fake Kafka broker that OME connects to.
type Broker struct {
	cfg      cfg.KafkaConfig
	log      *logrus.Logger
	listener net.Listener

	mu       sync.RWMutex
	topicSet map[string]bool

	msgCh chan Message
	done  chan struct{}
}

func New(c cfg.KafkaConfig, log *logrus.Logger) *Broker {
	ts := make(map[string]bool)
	for _, t := range c.Topics {
		ts[t] = true
	}
	return &Broker{
		cfg:      c,
		log:      log,
		topicSet: ts,
		msgCh:    make(chan Message, 4096),
		done:     make(chan struct{}),
	}
}

func (b *Broker) Messages() <-chan Message {
	return b.msgCh
}

func (b *Broker) Start() error {
	ln, err := net.Listen("tcp", b.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("broker listen %s: %w", b.cfg.ListenAddr, err)
	}
	b.listener = ln
	b.log.WithField("addr", b.cfg.ListenAddr).Info("Kafka broker listening")
	go b.acceptLoop()
	return nil
}

func (b *Broker) acceptLoop() {
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			select {
			case <-b.done:
				return
			default:
				b.log.WithError(err).Warn("accept error")
				continue
			}
		}
		b.log.WithField("remote", conn.RemoteAddr()).Debug("new Kafka connection")
		go b.handleConn(conn)
	}
}

func (b *Broker) handleConn(conn net.Conn) {
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		select {
		case <-b.done:
			return
		case <-ctx.Done():
			return
		default:
		}

		_ = conn.SetDeadline(time.Now().Add(5 * time.Minute))

		var size int32
		if err := binary.Read(conn, binary.BigEndian, &size); err != nil {
			if err != io.EOF {
				b.log.WithError(err).Debug("read request size")
			}
			return
		}
		if size <= 0 || size > 32*1024*1024 {
			b.log.WithField("size", size).Warn("invalid request size, closing")
			return
		}

		body := make([]byte, size)
		if _, err := io.ReadFull(conn, body); err != nil {
			b.log.WithError(err).Debug("read request body")
			return
		}

		respBytes, err := b.handleRequest(body)
		if err != nil {
			b.log.WithError(err).Warn("handle request error")
			return
		}
		if respBytes == nil {
			continue
		}

		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(respBytes)))
		if _, err := conn.Write(append(lenBuf, respBytes...)); err != nil {
			b.log.WithError(err).Debug("write response")
			return
		}
	}
}

func (b *Broker) handleRequest(body []byte) ([]byte, error) {
	if len(body) < 4 {
		return nil, fmt.Errorf("request too short")
	}
	r := &reader{buf: body}

	apiKey := r.readInt16()
	apiVersion := r.readInt16()
	correlationID := r.readInt32()

	isFlexible := isFlexibleVersion(apiKey, apiVersion)
	if isFlexible {
		r.readCompactString() // clientID
		r.readUvarint()       // tagged fields header
	} else {
		r.readString() // clientID
	}

	b.log.WithFields(logrus.Fields{
		"api_key":     apiKey,
		"api_version": apiVersion,
		"corr_id":     correlationID,
		"flexible":    isFlexible,
	}).Debug("kafka request")

	switch apiKey {
	case 18:
		return b.handleAPIVersions(correlationID, apiVersion)
	case 3:
		return b.handleMetadata(r, correlationID, apiVersion)
	case 0:
		return b.handleProduce(r, correlationID, apiVersion)
	case 17:
		return b.handleSaslHandshake(correlationID)
	case 36:
		return b.handleSaslAuthenticate(correlationID)
	default:
		b.log.WithField("api_key", apiKey).Debug("unsupported API key")
		return buildErrorResponse(correlationID, 35), nil
	}
}

func isFlexibleVersion(apiKey, apiVersion int16) bool {
	switch apiKey {
	case 18:
		return apiVersion >= 3
	case 3:
		return apiVersion >= 9
	case 0:
		return apiVersion >= 9
	default:
		return apiVersion >= 6
	}
}

// handleAPIVersions responds to ApiVersions requests (v0-v3).
// v3+ uses flexible/compact encoding per KIP-482.
func (b *Broker) handleAPIVersions(corrID int32, version int16) ([]byte, error) {
	type apiEntry struct{ key, min, max int16 }
	supported := []apiEntry{
		{0, 0, 8},
		{3, 0, 9},
		{18, 0, 3},
		{17, 0, 1},
		{36, 0, 2},
	}

	w := &writer{}
	w.writeInt32(corrID)
	w.writeInt16(0) // error_code: none

	if version >= 3 {
		// Flexible: compact array (N+1 uvarint)
		w.writeUvarint(uint64(len(supported) + 1))
		for _, e := range supported {
			w.writeInt16(e.key)
			w.writeInt16(e.min)
			w.writeInt16(e.max)
			w.writeUvarint(0) // tagged fields: empty
		}
		w.writeInt32(0)   // throttle_time_ms
		w.writeUvarint(0) // tagged fields: empty
	} else {
		w.writeInt32(int32(len(supported)))
		for _, e := range supported {
			w.writeInt16(e.key)
			w.writeInt16(e.min)
			w.writeInt16(e.max)
		}
		w.writeInt32(0) // throttle_time_ms
	}

	return w.bytes(), nil
}

func (b *Broker) handleMetadata(r *reader, corrID int32, version int16) ([]byte, error) {
	flexible := version >= 9

	var requestedTopics []string
	if flexible {
		n := int(r.readUvarint()) - 1
		for i := 0; i < n; i++ {
			requestedTopics = append(requestedTopics, r.readCompactString())
			r.readUvarint()
		}
		r.readUvarint()
	} else {
		n := r.readInt32()
		for i := int32(0); i < n; i++ {
			requestedTopics = append(requestedTopics, r.readString())
		}
	}

	if b.cfg.AutoCreateTopics {
		b.mu.Lock()
		for _, t := range requestedTopics {
			if t != "" && !b.topicSet[t] {
				b.topicSet[t] = true
				b.log.WithField("topic", t).Info("auto-created topic")
			}
		}
		b.mu.Unlock()
	}

	b.mu.RLock()
	topics := make([]string, 0, len(b.topicSet))
	for t := range b.topicSet {
		topics = append(topics, t)
	}
	b.mu.RUnlock()

	host := b.cfg.AdvertisedHost
	if host == "" {
		host = "localhost"
	}
	port := b.cfg.AdvertisedPort
	if port == 0 {
		port = 9092
	}

	w := &writer{}
	w.writeInt32(corrID)
	if flexible {
		w.writeUvarint(0) // response header tagged fields
	}
	w.writeInt32(0) // throttle_time_ms

	// Brokers
	if flexible {
		w.writeUvarint(2) // 1 broker
	} else {
		w.writeInt32(1)
	}
	w.writeInt32(1)
	if flexible {
		w.writeCompactString(host)
	} else {
		w.writeString(host)
	}
	w.writeInt32(int32(port))
	if flexible {
		w.writeUvarint(0) // rack: null
		w.writeUvarint(0) // tagged fields
	} else {
		w.writeInt16(-1) // rack: null
	}

	if version >= 2 {
		if flexible {
			w.writeUvarint(0) // clusterId: null
		} else {
			w.writeInt16(-1)
		}
	}
	if version >= 1 {
		w.writeInt32(1) // controllerId
	}

	// Topics
	if flexible {
		w.writeUvarint(uint64(len(topics) + 1))
	} else {
		w.writeInt32(int32(len(topics)))
	}
	for _, t := range topics {
		w.writeInt16(0)
		if flexible {
			w.writeCompactString(t)
		} else {
			w.writeString(t)
		}
		if version >= 1 {
			w.writeBool(false)
		}

		// 1 partition
		if flexible {
			w.writeUvarint(2)
		} else {
			w.writeInt32(1)
		}
		w.writeInt16(0) // partition error
		w.writeInt32(0) // partition index
		w.writeInt32(1) // leader
		if version >= 7 {
			w.writeInt32(0) // leader epoch
		}
		// replicas [1]
		if flexible {
			w.writeUvarint(2)
		} else {
			w.writeInt32(1)
		}
		w.writeInt32(1)
		// isr [1]
		if flexible {
			w.writeUvarint(2)
		} else {
			w.writeInt32(1)
		}
		w.writeInt32(1)
		// offline []
		if flexible {
			w.writeUvarint(1)
		} else {
			w.writeInt32(0)
		}
		if flexible {
			w.writeUvarint(0) // partition tagged fields
			w.writeUvarint(0) // topic tagged fields
		}
	}

	if version >= 8 {
		w.writeInt32(0) // cluster authorized ops
	}
	if flexible {
		w.writeUvarint(0) // response tagged fields
	}

	return w.bytes(), nil
}

func (b *Broker) handleProduce(r *reader, corrID int32, apiVersion int16) ([]byte, error) {
	flexible := apiVersion >= 9

	if flexible {
		r.readCompactNullableString()
	} else {
		r.readString()
	}
	_ = r.readInt16()
	_ = r.readInt32()

	var nTopics int32
	if flexible {
		nTopics = int32(r.readUvarint()) - 1
	} else {
		nTopics = r.readInt32()
	}

	for i := int32(0); i < nTopics; i++ {
		var topic string
		if flexible {
			topic = r.readCompactString()
		} else {
			topic = r.readString()
		}

		var nPartitions int32
		if flexible {
			nPartitions = int32(r.readUvarint()) - 1
		} else {
			nPartitions = r.readInt32()
		}

		for j := int32(0); j < nPartitions; j++ {
			partition := r.readInt32()
			_ = r.readInt32()

			msgs, err := b.decodeRecordBatch(r, topic, partition, apiVersion)
			if err != nil {
				b.log.WithError(err).WithField("topic", topic).Warn("decode record batch")
				continue
			}
			for _, m := range msgs {
				select {
				case b.msgCh <- m:
				default:
					b.log.Warn("message channel full, dropping record")
				}
			}
			if flexible {
				r.readUvarint()
			}
		}
		if flexible {
			r.readUvarint()
		}
	}
	if flexible {
		r.readUvarint()
	}

	w := &writer{}
	w.writeInt32(corrID)
	if flexible {
		w.writeUvarint(0)
	}
	w.writeInt32(0) // throttle_time_ms
	if flexible {
		w.writeUvarint(1) // empty compact array
		w.writeUvarint(0) // tagged fields
	} else {
		w.writeInt32(0)
	}
	return w.bytes(), nil
}

func (b *Broker) handleSaslHandshake(corrID int32) ([]byte, error) {
	w := &writer{}
	w.writeInt32(corrID)
	w.writeInt16(33) // UNSUPPORTED_SASL_MECHANISM
	w.writeInt32(0)
	return w.bytes(), nil
}

func (b *Broker) handleSaslAuthenticate(corrID int32) ([]byte, error) {
	w := &writer{}
	w.writeInt32(corrID)
	w.writeInt16(58) // SASL_AUTHENTICATION_FAILED
	w.writeInt16(-1)
	w.writeInt32(-1)
	return w.bytes(), nil
}

// ─── Record batch decoder ─────────────────────────────────────────────────────

func (b *Broker) decodeRecordBatch(r *reader, topic string, partition int32, _ int16) ([]Message, error) {
	if r.remaining() < 17 {
		return nil, nil
	}
	pos := r.pos
	_ = r.readInt64()
	_ = r.readInt32()
	_ = r.readInt32()
	magic := r.readInt8()

	if magic == 2 {
		return b.decodeRecordBatchV2(r, topic, partition)
	}
	r.pos = pos
	return b.decodeLegacyMessageSet(r, topic, partition)
}

func (b *Broker) decodeRecordBatchV2(r *reader, topic string, partition int32) ([]Message, error) {
	_ = r.readInt16()
	_ = r.readInt32()
	baseTimestamp := r.readInt64()
	_ = r.readInt64()
	_ = r.readInt64()
	_ = r.readInt16()
	_ = r.readInt32()

	nRecords := r.readInt32()
	msgs := make([]Message, 0, nRecords)
	for i := int32(0); i < nRecords && r.remaining() > 0; i++ {
		_ = r.readVarint()
		_ = r.readInt8()
		_ = r.readVarint()
		_ = r.readVarint()
		key := r.readVarBytes()
		value := r.readVarBytes()
		nH := r.readVarint()
		for h := int64(0); h < nH; h++ {
			r.readVarBytes()
			r.readVarBytes()
		}
		msgs = append(msgs, Message{
			Topic: topic, Partition: partition,
			Key: key, Value: value,
			Timestamp: time.UnixMilli(baseTimestamp),
		})
	}
	return msgs, nil
}

func (b *Broker) decodeLegacyMessageSet(r *reader, topic string, partition int32) ([]Message, error) {
	var msgs []Message
	for r.remaining() >= 12 {
		_ = r.readInt64()
		size := r.readInt32()
		if size <= 0 || int(size) > r.remaining() {
			break
		}
		_ = r.readInt32()
		_ = r.readInt8()
		_ = r.readInt8()
		key := r.readBytes()
		value := r.readBytes()
		msgs = append(msgs, Message{
			Topic: topic, Partition: partition,
			Key: key, Value: value, Timestamp: time.Now(),
		})
	}
	return msgs, nil
}

func buildErrorResponse(corrID int32, errCode int16) []byte {
	w := &writer{}
	w.writeInt32(corrID)
	w.writeInt16(errCode)
	return w.bytes()
}

func (b *Broker) Close() {
	close(b.done)
	if b.listener != nil {
		_ = b.listener.Close()
	}
}

// ─── Binary reader ────────────────────────────────────────────────────────────

type reader struct {
	buf []byte
	pos int
}

func (r *reader) remaining() int { return len(r.buf) - r.pos }

func (r *reader) readInt8() int8 {
	if r.pos >= len(r.buf) {
		return 0
	}
	v := int8(r.buf[r.pos])
	r.pos++
	return v
}

func (r *reader) readInt16() int16 {
	if r.pos+2 > len(r.buf) {
		return 0
	}
	v := int16(binary.BigEndian.Uint16(r.buf[r.pos:]))
	r.pos += 2
	return v
}

func (r *reader) readInt32() int32 {
	if r.pos+4 > len(r.buf) {
		return 0
	}
	v := int32(binary.BigEndian.Uint32(r.buf[r.pos:]))
	r.pos += 4
	return v
}

func (r *reader) readInt64() int64 {
	if r.pos+8 > len(r.buf) {
		return 0
	}
	v := int64(binary.BigEndian.Uint64(r.buf[r.pos:]))
	r.pos += 8
	return v
}

func (r *reader) readString() string {
	n := r.readInt16()
	if n < 0 || r.pos+int(n) > len(r.buf) {
		return ""
	}
	s := string(r.buf[r.pos : r.pos+int(n)])
	r.pos += int(n)
	return s
}

func (r *reader) readCompactString() string {
	n := int(r.readUvarint()) - 1
	if n < 0 || r.pos+n > len(r.buf) {
		return ""
	}
	s := string(r.buf[r.pos : r.pos+n])
	r.pos += n
	return s
}

func (r *reader) readCompactNullableString() string {
	n := int(r.readUvarint()) - 1
	if n < 0 || r.pos+n > len(r.buf) {
		return ""
	}
	s := string(r.buf[r.pos : r.pos+n])
	r.pos += n
	return s
}

func (r *reader) readBytes() []byte {
	n := r.readInt32()
	if n < 0 || r.pos+int(n) > len(r.buf) {
		return nil
	}
	b := make([]byte, n)
	copy(b, r.buf[r.pos:r.pos+int(n)])
	r.pos += int(n)
	return b
}

func (r *reader) readUvarint() uint64 {
	v, n := binary.Uvarint(r.buf[r.pos:])
	if n > 0 {
		r.pos += n
	}
	return v
}

func (r *reader) readVarint() int64 {
	v, n := binary.Varint(r.buf[r.pos:])
	if n > 0 {
		r.pos += n
	}
	return v
}

func (r *reader) readVarBytes() []byte {
	n := r.readVarint()
	if n < 0 || r.pos+int(n) > len(r.buf) {
		return nil
	}
	b := make([]byte, n)
	copy(b, r.buf[r.pos:r.pos+int(n)])
	r.pos += int(n)
	return b
}

// ─── Binary writer ────────────────────────────────────────────────────────────

type writer struct{ buf []byte }

func (w *writer) writeInt8(v int8)  { w.buf = append(w.buf, byte(v)) }
func (w *writer) writeBool(v bool)  {
	if v {
		w.writeInt8(1)
	} else {
		w.writeInt8(0)
	}
}
func (w *writer) bytes() []byte { return w.buf }

func (w *writer) writeInt16(v int16) {
	b := [2]byte{}
	binary.BigEndian.PutUint16(b[:], uint16(v))
	w.buf = append(w.buf, b[:]...)
}

func (w *writer) writeInt32(v int32) {
	b := [4]byte{}
	binary.BigEndian.PutUint32(b[:], uint32(v))
	w.buf = append(w.buf, b[:]...)
}

func (w *writer) writeInt64(v int64) {
	b := [8]byte{}
	binary.BigEndian.PutUint64(b[:], uint64(v))
	w.buf = append(w.buf, b[:]...)
}

func (w *writer) writeString(s string) {
	if s == "" {
		w.writeInt16(-1)
		return
	}
	w.writeInt16(int16(len(s)))
	w.buf = append(w.buf, s...)
}

func (w *writer) writeCompactString(s string) {
	w.writeUvarint(uint64(len(s) + 1))
	w.buf = append(w.buf, s...)
}

func (w *writer) writeCompactNullableString(s string) {
	if s == "" {
		w.writeUvarint(0)
		return
	}
	w.writeUvarint(uint64(len(s) + 1))
	w.buf = append(w.buf, s...)
}

func (w *writer) writeUvarint(v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	w.buf = append(w.buf, tmp[:n]...)
}