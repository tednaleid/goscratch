package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type partitionInfo struct {
	topic           string
	partition       int32
	currentOffset   int64
	committedOffset int64
	lag             int64
	lastTimestamp   time.Time
	firstTimestamp  time.Time
	lastOffset      int64
	firstOffset     int64
	mutex           sync.Mutex
}

type kafkaLagMonitor struct {
	client           sarama.Client
	admin            sarama.ClusterAdmin
	consumer         sarama.Consumer
	partitionInfoMap map[string]*partitionInfo
	mapMutex         sync.Mutex
	groupID          string
	topic            string
	partition        int
}

func main() {
	broker, groupID, topic, partition := parseFlags()

	monitor, err := newKafkaLagMonitor(broker, groupID, topic, partition)
	if err != nil {
		log.Fatalf("Error creating Kafka lag monitor: %v", err)
	}
	defer monitor.close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go monitor.consumeOffsets(ctx)
	go monitor.displayStats(ctx)

	waitForInterrupt(cancel)
	fmt.Println("\nShutting down...")
}

func parseFlags() (string, string, string, int) {
	broker := flag.String("b", "", "Kafka broker address (host:port)")
	groupID := flag.String("g", "", "Consumer group ID")
	topic := flag.String("t", "", "Kafka topic (optional)")
	partition := flag.Int("p", -1, "Kafka topic partition (optional)")
	flag.Parse()

	if *broker == "" || *groupID == "" {
		flag.Usage()
		os.Exit(1)
	}

	return *broker, *groupID, *topic, *partition
}

func newKafkaLagMonitor(broker, groupID, topic string, partition int) (*kafkaLagMonitor, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	fmt.Fprintf(os.Stderr, "broker: %s group: %s, topic: %s, partition: %d\n", broker, groupID, topic, partition)

	client, err := sarama.NewClient([]string{broker}, config)
	if err != nil {
		return nil, fmt.Errorf("error creating client: %v", err)
	}

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("error creating admin client: %v", err)
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		admin.Close()
		client.Close()
		return nil, fmt.Errorf("error creating consumer: %v", err)
	}

	return &kafkaLagMonitor{
		client:           client,
		admin:            admin,
		consumer:         consumer,
		partitionInfoMap: make(map[string]*partitionInfo),
		groupID:          groupID,
		topic:            topic,
		partition:        partition,
	}, nil
}

func (m *kafkaLagMonitor) close() {
	m.consumer.Close()
	m.admin.Close()
	m.client.Close()
}

func (m *kafkaLagMonitor) consumeOffsets(ctx context.Context) {
	consumerOffsetsTopic := "__consumer_offsets"
	partitionToConsume := abs(stringHashCode(m.groupID) % 50)

	fmt.Fprintf(os.Stderr, "partitionToConsume: %d\n", partitionToConsume)

	partitionConsumer, err := m.consumer.ConsumePartition(consumerOffsetsTopic, int32(partitionToConsume), sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Error creating partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			m.processMessage(msg)
		case <-ctx.Done():
			return
		}
	}
}

func (m *kafkaLagMonitor) processMessage(msg *sarama.ConsumerMessage) {
	key, err := parseKey(msg.Key)
	if err != nil {
		log.Fatalf("Error parsing key: %v", err)
	}

	fmt.Println(key)

	if key.Group == m.groupID && (m.topic == "" || m.topic == key.Topic) && (m.partition == -1 || int32(m.partition) == key.Partition) {
		value, err := parseValue(msg.Value)
		if err != nil {
			log.Fatalf("Error parsing value: %v", err)
		}

		fmt.Println(value)

		//m.mapMutex.Lock()
		//key := fmt.Sprintf("%s-%d", key.Topic, key.Partition)
		//info, ok := m.partitionInfoMap[key]
		//if !ok {
		//	info = &partitionInfo{
		//		topic:     key.Topic,
		//		partition: key.Partition,
		//	}
		//	m.partitionInfoMap[key] = info
		//}
		//m.mapMutex.Unlock()
		//
		//info.mutex.Lock()
		//info.committedOffset = offset
		//currentTime := time.Now()
		//if info.firstTimestamp.IsZero() {
		//	info.firstTimestamp = currentTime
		//	info.firstOffset = offset
		//}
		//info.lastTimestamp = currentTime
		//info.lastOffset = offset
		//info.mutex.Unlock()
	}
}

type KeyInfo struct {
	Version   int64
	Group     string
	Topic     string
	Partition int32
}

func (k KeyInfo) String() string {
	return fmt.Sprintf("Version: %d, Group: %s, Topic: %s, Partition: %d", k.Version, k.Group, k.Topic, k.Partition)
}

func parseKey(key []byte) (KeyInfo, error) {
	var info KeyInfo

	version, err := binary.ReadVarint(bytes.NewBuffer(key))
	if err != nil {
		return info, fmt.Errorf("error reading version: %v", err)
	}
	info.Version = version

	if version == 0 || version == 1 {
		group, err := decodeCompactString(key[2:])
		if err != nil {
			return info, fmt.Errorf("error decoding group: %v", err)
		}
		info.Group = group

		topic, err := decodeCompactString(key[2+len(group):])
		if err != nil {
			return info, fmt.Errorf("error decoding topic: %v", err)
		}
		info.Topic = topic

		partition := binary.BigEndian.Uint32(key[2+len(group)+len(topic):])
		info.Partition = int32(partition)
	}

	return info, nil
}

type ValueInfo struct {
	Version   int64
	Offset    int64
	Metadata  string
	Timestamp time.Time
}

func (v ValueInfo) String() string {
	return fmt.Sprintf("Version: %d, Offset: %d, Metadata: %s, Timestamp: %s", v.Version, v.Offset, v.Metadata, v.Timestamp)
}

func parseValue(value []byte) (ValueInfo, error) {
	var info ValueInfo

	version, err := binary.ReadVarint(bytes.NewBuffer(value))
	if err != nil {
		return info, fmt.Errorf("error reading version: %v", err)
	}
	info.Version = version

	offset := binary.BigEndian.Uint64(value[2:])
	info.Offset = int64(offset)

	metadata, err := decodeCompactString(value[10:])
	if err != nil {
		return info, fmt.Errorf("error decoding metadata: %v", err)
	}
	info.Metadata = metadata

	timestampMillis := binary.BigEndian.Uint64(value[10+len(metadata):])
	timestampSecs := int64(timestampMillis / 1000)
	timestampNanos := int64((timestampMillis % 1000) * 1e6) // Convert remaining milliseconds to nanoseconds
	info.Timestamp = time.Unix(timestampSecs, timestampNanos)

	return info, nil
}

func decodeCompactString(data []byte) (string, error) {
	length, bytesRead := binary.Uvarint(data)
	if bytesRead <= 0 {
		return "", fmt.Errorf("error decoding string length")
	}
	str := string(data[bytesRead : bytesRead+int(length)])
	return str, nil
}

func (m *kafkaLagMonitor) displayStats(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.updateAndPrintStats()
		case <-ctx.Done():
			return
		}
	}
}

func (m *kafkaLagMonitor) updateAndPrintStats() {
	m.mapMutex.Lock()
	defer m.mapMutex.Unlock()

	for _, info := range m.partitionInfoMap {
		info.mutex.Lock()
		currentOffset, err := m.client.GetOffset(info.topic, info.partition, sarama.OffsetNewest)
		if err != nil {
			log.Printf("Error getting current offset: %v", err)
			info.mutex.Unlock()
			continue
		}
		info.currentOffset = currentOffset
		info.lag = currentOffset - info.committedOffset

		timeDiff := info.lastTimestamp.Sub(info.firstTimestamp).Seconds()
		totalRate := float64(info.lastOffset-info.firstOffset) / timeDiff
		instantRate := 0.0
		if info.lastTimestamp != info.firstTimestamp {
			instantRate = float64(info.lastOffset-info.firstOffset) / info.lastTimestamp.Sub(info.firstTimestamp).Seconds()
		}

		fmt.Printf("\rTopic: %s, Partition: %d, Current: %d, Committed: %d, Lag: %d, Instant Rate: %.2f/s, Total Rate: %.2f/s",
			info.topic, info.partition, info.currentOffset, info.committedOffset, info.lag, instantRate, totalRate)
		info.mutex.Unlock()
	}
}

func waitForInterrupt(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
	cancel()
}

func stringHashCode(s string) int32 {
	var hash int32 = 0
	for i := 0; i < len(s); i++ {
		hash = 31*hash + int32(s[i])
	}
	return hash
}

func abs(x int32) int32 {
	if x < 0 {
		return -x
	}
	return x
}
