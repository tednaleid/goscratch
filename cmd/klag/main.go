package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/IBM/sarama"
)

type kafkaLagMonitor struct {
	client    sarama.Client
	admin     sarama.ClusterAdmin
	consumer  sarama.Consumer
	groupID   string
	topic     string
	partition int
	kvChannel chan KeyAndValue // Add this line
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

	go monitor.consumeGroupIdProgress(ctx)
	go monitor.emitGroupIdProgress()

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
		client:    client,
		admin:     admin,
		consumer:  consumer,
		groupID:   groupID,
		topic:     topic,
		partition: partition,
		kvChannel: make(chan KeyAndValue),
	}, nil
}

func (m *kafkaLagMonitor) close() {
	m.consumer.Close()
	m.admin.Close()
	m.client.Close()
	close(m.kvChannel)
}

type PartitionInfo struct {
	firstOffset, lastOffset       int64
	firstTimestamp, lastTimestamp time.Time
}

func (m *kafkaLagMonitor) emitGroupIdProgress() {
	partitionData := make(map[string]*PartitionInfo)

	for kv := range m.kvChannel {
		key := fmt.Sprintf("%s-%d", kv.Key.Topic, kv.Key.Partition)

		if _, ok := partitionData[key]; !ok {
			partitionData[key] = &PartitionInfo{
				firstOffset:    kv.Value.Offset,
				lastOffset:     kv.Value.Offset,
				firstTimestamp: kv.Value.Timestamp,
				lastTimestamp:  kv.Value.Timestamp,
			}
		}

		partitionInfo := partitionData[key]

		recordsSinceLastCommit := kv.Value.Offset - partitionInfo.lastOffset
		elapsedSinceLastCommit := kv.Value.Timestamp.Sub(partitionInfo.lastTimestamp).Truncate(time.Second)

		var recordsPerSecondSinceLastCommit float64
		if elapsedSinceLastCommit.Seconds() > 0 {
			recordsPerSecondSinceLastCommit = float64(recordsSinceLastCommit) / elapsedSinceLastCommit.Seconds()
		} else {
			recordsPerSecondSinceLastCommit = 0
		}

		fmt.Printf("%s %3d %s %d +%d %s %.2f/s\n", kv.Key.Topic, kv.Key.Partition, kv.Value.Timestamp, kv.Value.Offset, recordsSinceLastCommit, elapsedSinceLastCommit, recordsPerSecondSinceLastCommit)

		partitionData[key].lastOffset = kv.Value.Offset
		partitionData[key].lastTimestamp = kv.Value.Timestamp
	}
}

func (m *kafkaLagMonitor) consumeGroupIdProgress(ctx context.Context) {
	consumerOffsetsTopic := "__consumer_offsets"
	consumerOffsetsPartitionCount := 50
	partitionToConsume := abs(stringHashCode(m.groupID) % int32(consumerOffsetsPartitionCount))

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
		// invalid key, ignore
		return
	}

	if key.Group == m.groupID && (m.topic == "" || m.topic == key.Topic) && (m.partition == -1 || int32(m.partition) == key.Partition) {
		value, err := parseValue(msg.Value)
		if err != nil {
			log.Printf("Error parsing value: %v\n", err)
			return
		}
		m.kvChannel <- KeyAndValue{Key: key, Value: value}
	}
}

type KeyAndValue struct {
	Key   KeyInfo
	Value ValueInfo
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

// kafka source code for keys:
// https://github.com/apache/kafka/blob/8152ee6519f1c2d0608702000ed9c0b1162c447d/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala#L1077
func parseKey(key []byte) (KeyInfo, error) {
	if len(key) < 2 {
		return KeyInfo{}, errors.New("key is too short")
	}

	version := int16(binary.BigEndian.Uint16(key[:2]))
	key = key[2:]

	switch version {
	case 0, 1:
		return parseKeyV0V1(key, int64(version))
	case 2:
		return parseKeyV2(key)
	default:
		return KeyInfo{}, fmt.Errorf("unsupported version: %d", version)
	}
}

func parseKeyV0V1(key []byte, version int64) (KeyInfo, error) {
	group, key, err := decodeString(key)
	if err != nil {
		return KeyInfo{}, err
	}

	topic, key, err := decodeString(key)
	if err != nil {
		return KeyInfo{}, err
	}

	if len(key) < 4 {
		return KeyInfo{}, errors.New("key is too short for partition")
	}
	partition := int32(binary.BigEndian.Uint32(key))

	return KeyInfo{
		Version:   version,
		Group:     group,
		Topic:     topic,
		Partition: partition,
	}, nil
}

func parseKeyV2(key []byte) (KeyInfo, error) {
	group, key, err := decodeString(key)
	if err != nil {
		return KeyInfo{}, err
	}

	topic, key, err := decodeString(key)
	if err != nil {
		return KeyInfo{}, err
	}

	if len(key) < 4 {
		return KeyInfo{}, errors.New("key is too short for partition")
	}
	partition := int32(binary.BigEndian.Uint32(key))

	return KeyInfo{
		Version:   2,
		Group:     group,
		Topic:     topic,
		Partition: partition,
	}, nil
}

func decodeString(data []byte) (string, []byte, error) {
	if len(data) < 2 {
		return "", nil, errors.New("data is too short for string length")
	}
	length := int(binary.BigEndian.Uint16(data[:2]))
	data = data[2:]

	if len(data) < length {
		return "", nil, fmt.Errorf("data is too short for string content: need %d, have %d", length, len(data))
	}
	return string(data[:length]), data[length:], nil
}

type ValueInfo struct {
	Version   int64
	Offset    int64
	Timestamp time.Time
}

func (v ValueInfo) String() string {
	return fmt.Sprintf("Version: %d, Offset: %d, Timestamp: %s", v.Version, v.Offset, v.Timestamp)
}

// source code for values:
// https://github.com/apache/kafka/blob/8152ee6519f1c2d0608702000ed9c0b1162c447d/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala#L1090
func parseValue(value []byte) (ValueInfo, error) {
	if len(value) < 2 {
		return ValueInfo{}, errors.New("value is too short")
	}

	version := int16(binary.BigEndian.Uint16(value[:2]))
	value = value[2:]

	switch version {
	case 3:
		return parseValueV3(value, int64(version))
	default:
		return ValueInfo{}, fmt.Errorf("unsupported version: %d", version)
	}
}

func parseValueV3(value []byte, version int64) (ValueInfo, error) {
	valueLength := len(value)
	if valueLength < 16 {
		return ValueInfo{}, errors.New("value is too short for offset and commit timestamp")
	}

	offsetBytes := value[:8]
	commitTimestampBytes := value[(valueLength - 8):]

	offset := int64(binary.BigEndian.Uint64(offsetBytes))
	epochTime := int64(binary.BigEndian.Uint64(commitTimestampBytes))
	commitTimestamp := time.Unix(0, epochTime*int64(time.Millisecond))

	valueInfo := ValueInfo{
		Version:   version,
		Offset:    offset,
		Timestamp: commitTimestamp,
	}

	//fmt.Printf("value length: %d, value: %x %x %x %s\n", len(value), offsetBytes, metadataBytes, commitTimestampBytes, valueInfo)
	return valueInfo, nil
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
