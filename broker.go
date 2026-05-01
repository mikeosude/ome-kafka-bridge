// Package broker implements a minimal Kafka broker sufficient for Dell
// OpenManage Enterprise to connect to as a producer.
//
// OME uses the Kafka producer client (librdkafka under the hood) which performs:
//   1. TCP connect to bootstrap server
//   2. ApiVersions request        → we reply with supported versions
//   3. Metadata request           → we reply with broker/topic metadata
//   4. Produce requests           → we decode and deliver to the pipeline
//
// We implement exactly these four request types (+ mandatory responses) and
// return UNSUPPORTED_VERSION for everything else. This is enough to receive
// all OME telemetry with PLAINTEXT security (no SASL/SSL in this version;
// see README for how to front this with a TLS proxy).
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

	cfg "github.com/ifesi/ome-kafka-bridge/internal/config"
)

// Message is a fully decoded Kafka ProduceRequest record delivered to the
// pipeline.
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

	// topicSet is the set of known topics (auto-created or pre-configured).
	mu       sync.RWMutex
	topicSet map[string]bool

	// msgCh receives decoded messages for the pipeline.
	msgCh chan Message

	done chan struct{}
}

// New creates a Broker but does not start it.
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

// Messages returns the channel on which decoded produce records are delivered.
func (b *Broker) Messages() <-chan Message {
	return b.msgCh
}

// Start begins listening for Kafka producer connections.
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

// ─── Connection handler ───────────────────────────────────────────────────────

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

		// Read request length (4 bytes big-endian)
		_ = conn.SetDeadline(time.Now().Add(5 * time.Minute))
		var size int32
		if err := binary.Read(conn, binary.BigEndian, &size); err != nil {
			if err != io.EOF {
				b.log.WithError(err).Debug("read request size")
			}
			return
		}
		if size <= 0 || size > 32*1024*1024 {
			b.log.WithField("size", size).Warn("invalid request size, closing connection")
			return
		}

		body := make([]byte, size)
		if _, err := io.ReadFull(conn, body); err != nil {
			b.log.WithError(err).Debug("read request body")
			return
		}

		respBytes, err := b.handleRequest(body)
		if err != nil {
			b.log.WithError(err).Warn("handle request")
			return
		}
		if respBytes == nil {
			continue // no response needed
		}

		// Write response: 4-byte length prefix + body
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(respBytes)))
		if _, err := conn.Write(append(lenBuf, respBytes...)); err != nil {
			b.log.WithError(err).Debug("write response")
			return
		}
	}
}

// ─── Request dispatcher ───────────────────────────────────────────────────────

func (b *Broker) handleRequest(body []byte) ([]byte, error) {
	if len(body) < 4 {
		return nil, fmt.Errorf("request too short: %d bytes", len(body))
	}
	r := &reader{buf: body}

	apiKey := r.readInt16()
	apiVersion := r.readInt16()
	correlationID := r.readInt32()
	_ = r.readString() // clientID (ignored)

	b.log.WithFields(logrus.Fields{
		"api_key":        apiKey,
		"api_version":    apiVersion,
		"correlation_id": correlationID,
	}).Trace("kafka request")

	switch apiKey {
	case 18: // ApiVersions
		return b.handleAPIVersions(correlationID, apiVersion)
	case 3: // Metadata
		return b.handleMetadata(r, correlationID)
	case 0: // Produce
		return b.handleProduce(r, correlationID, apiVersion)
	default:
		// Return UNSUPPORTED_VERSION for everything else
		return buildErrorResponse(correlationID, 35), nil
	}
}

// ─── ApiVersions (key 18) ─────────────────────────────────────────────────────

// We advertise support for the three API keys we actually handle.
func (b *Broker) handleAPIVersions(corrID int32, _ int16) ([]byte, error) {
	w := &writer{}
	w.writeInt32(corrID)
	w.writeInt16(0) // error code: none

	// Supported API keys: [apiKey, minVersion, maxVersion]
	supported := [][3]int16{
		{0, 0, 8},  // Produce
		{3, 0, 9},  // Metadata
		{18, 0, 3}, // ApiVersions
	}
	w.writeInt32(int32(len(supported)))
	for _, entry := range supported {
		w.writeInt16(entry[0])
		w.writeInt16(entry[1])
		w.writeInt16(entry[2])
	}
	w.writeInt32(0) // throttle_time_ms
	return w.bytes(), nil
}

// ─── Metadata (key 3) ─────────────────────────────────────────────────────────

func (b *Broker) handleMetadata(r *reader, corrID int32) ([]byte, error) {
	// Parse requested topics
	nTopics := r.readInt32()
	var requestedTopics []string
	for i := int32(0); i < nTopics; i++ {
		requestedTopics = append(requestedTopics, r.readString())
	}

	// Auto-create topics if configured
	if b.cfg.AutoCreateTopics {
		b.mu.Lock()
		for _, t := range requestedTopics {
			if !b.topicSet[t] {
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
	w.writeInt32(0) // throttle_time_ms

	// Brokers array (just us)
	w.writeInt32(1)
	w.writeInt32(1)      // nodeId
	w.writeString(host)  // host
	w.writeInt32(int32(port))
	w.writeString("") // rack (nullable)

	// Controller ID
	w.writeInt32(1)

	// Topics
	w.writeInt32(int32(len(topics)))
	for _, t := range topics {
		w.writeInt16(0) // error code
		w.writeString(t)
		w.writeBool(false) // isInternal

		// Partitions (1 partition per topic)
		w.writeInt32(1)
		w.writeInt16(0)  // partition error
		w.writeInt32(0)  // partition index
		w.writeInt32(1)  // leader node
		w.writeInt32(1)  // leader epoch

		// Replica nodes
		w.writeInt32(1)
		w.writeInt32(1)

		// In-sync replicas
		w.writeInt32(1)
		w.writeInt32(1)

		// Offline replicas
		w.writeInt32(0)
	}

	w.writeBool(false) // cluster authorized ops
	return w.bytes(), nil
}

// ─── Produce (key 0) ──────────────────────────────────────────────────────────

func (b *Broker) handleProduce(r *reader, corrID int32, apiVersion int16) ([]byte, error) {
	_ = r.readInt16() // transactional_id (nullable, skip)
	_ = r.readInt16() // acks
	_ = r.readInt32() // timeout_ms

	nTopics := r.readInt32()
	for i := int32(0); i < nTopics; i++ {
		topic := r.readString()
		nPartitions := r.readInt32()
		for j := int32(0); j < nPartitions; j++ {
			partition := r.readInt32()
			_ = r.readInt32() // record_set size (bytes)

			// Decode MessageSet / RecordBatch
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
		}
	}

	// Produce response
	w := &writer{}
	w.writeInt32(corrID)
	w.writeInt32(0) // throttle_time_ms

	// We acknowledge all produce responses as success (offset 0)
	// OME only checks for connection/error, not exact offsets.
	return w.bytes(), nil
}

// decodeRecordBatch handles Kafka RecordBatch format (magic byte 2, API v3+)
// and falls back to legacy MessageSet (magic byte 0/1).
func (b *Broker) decodeRecordBatch(r *reader, topic string, partition int32, _ int16) ([]Message, error) {
	// Peek magic byte: in RecordBatch format, magic is at offset 16 of the batch.
	// Since r already points past the size field, save position.
	pos := r.pos

	// Try RecordBatch (magic = 2)
	// Format: baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) + magic(1)
	if r.remaining() < 17 {
		return nil, nil
	}
	_ = r.readInt64() // baseOffset
	_ = r.readInt32() // batchLength
	_ = r.readInt32() // partitionLeaderEpoch
	magic := r.readInt8()

	if magic == 2 {
		return b.decodeRecordBatchV2(r, topic, partition)
	}

	// Legacy MessageSet (magic 0 or 1)
	r.pos = pos
	return b.decodeLegacyMessageSet(r, topic, partition)
}

// decodeRecordBatchV2 decodes a Kafka RecordBatch (magic=2).
func (b *Broker) decodeRecordBatchV2(r *reader, topic string, partition int32) ([]Message, error) {
	_ = r.readInt16() // attributes
	_ = r.readInt32() // lastOffsetDelta
	baseTimestamp := r.readInt64()
	_ = r.readInt64() // maxTimestamp
	_ = r.readInt64() // producerId
	_ = r.readInt16() // producerEpoch
	_ = r.readInt32() // baseSequence

	nRecords := r.readInt32()
	msgs := make([]Message, 0, nRecords)

	for i := int32(0); i < nRecords && r.remaining() > 0; i++ {
		_ = r.readVarint()          // record length
		_ = r.readInt8()            // attributes
		_ = r.readVarint()          // timestampDelta
		_ = r.readVarint()          // offsetDelta
		key := r.readVarBytes()     // key
		value := r.readVarBytes()   // value
		nHeaders := r.readVarint()  // headers
		for h := int64(0); h < nHeaders; h++ {
			r.readVarBytes()
			r.readVarBytes()
		}

		msgs = append(msgs, Message{
			Topic:     topic,
			Partition: partition,
			Key:       key,
			Value:     value,
			Timestamp: time.UnixMilli(baseTimestamp),
		})
	}
	return msgs, nil
}

// decodeLegacyMessageSet decodes old-style MessageSet.
func (b *Broker) decodeLegacyMessageSet(r *reader, topic string, partition int32) ([]Message, error) {
	var msgs []Message
	for r.remaining() > 0 {
		if r.remaining() < 12 {
			break
		}
		_ = r.readInt64()   // offset
		size := r.readInt32()
		if int(size) > r.remaining() {
			break
		}
		msgStart := r.pos
		_ = r.readInt32()   // crc
		_ = r.readInt8()    // magic
		attrs := r.readInt8()
		_ = attrs // compression not handled in bridge

		var ts time.Time
		// magic 1 has timestamp
		// We already consumed magic above, just use now
		ts = time.Now()

		key := r.readBytes()
		value := r.readBytes()

		_ = msgStart // consumed
		msgs = append(msgs, Message{
			Topic:     topic,
			Partition: partition,
			Key:       key,
			Value:     value,
			Timestamp: ts,
		})
	}
	return msgs, nil
}

// ─── Low-level helpers ────────────────────────────────────────────────────────

func buildErrorResponse(corrID int32, errCode int16) []byte {
	w := &writer{}
	w.writeInt32(corrID)
	w.writeInt16(errCode)
	return w.bytes()
}

// Close shuts down the broker.
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

func (r *reader) remaining() int {
	return len(r.buf) - r.pos
}

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
	if n < 0 {
		return "" // nullable
	}
	if r.pos+int(n) > len(r.buf) {
		return ""
	}
	s := string(r.buf[r.pos : r.pos+int(n)])
	r.pos += int(n)
	return s
}

func (r *reader) readBytes() []byte {
	n := r.readInt32()
	if n < 0 {
		return nil
	}
	if r.pos+int(n) > len(r.buf) {
		return nil
	}
	b := make([]byte, n)
	copy(b, r.buf[r.pos:r.pos+int(n)])
	r.pos += int(n)
	return b
}

func (r *reader) readVarint() int64 {
	v, n := binary.Varint(r.buf[r.pos:])
	r.pos += n
	return v
}

func (r *reader) readVarBytes() []byte {
	n := r.readVarint()
	if n < 0 {
		return nil
	}
	if r.pos+int(n) > len(r.buf) {
		return nil
	}
	b := make([]byte, n)
	copy(b, r.buf[r.pos:r.pos+int(n)])
	r.pos += int(n)
	return b
}

// ─── Binary writer ────────────────────────────────────────────────────────────

type writer struct {
	buf []byte
}

func (w *writer) writeInt8(v int8) {
	w.buf = append(w.buf, byte(v))
}

func (w *writer) writeInt16(v int16) {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(v))
	w.buf = append(w.buf, b...)
}

func (w *writer) writeInt32(v int32) {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	w.buf = append(w.buf, b...)
}

func (w *writer) writeString(s string) {
	if s == "" {
		w.writeInt16(-1) // nullable
		return
	}
	w.writeInt16(int16(len(s)))
	w.buf = append(w.buf, s...)
}

func (w *writer) writeBool(v bool) {
	if v {
		w.writeInt8(1)
	} else {
		w.writeInt8(0)
	}
}

func (w *writer) bytes() []byte {
	return w.buf
}

func (w *writer) writeInt64(v int64) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	w.buf = append(w.buf, b...)
}
