package pers.jasonLbase.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerClient<K, V> {
	private String bootstrapServers;
	private String acks;
	private long deliveryTimeoutMS;
	private long batchSize;
	private long lingerMS;
	private long bufferMemory;
	private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
	private String valueSerializer = "org.apache.kafka.common.serialization.ByteArraySerializer";

	private Producer<K, V> producer;

	public void init() {
		Properties props = new Properties();

		if (this.bootstrapServers != null)
			props.put("bootstrap.servers", this.bootstrapServers);

		if (this.acks != null)
			props.put("acks", this.acks);

		if (this.deliveryTimeoutMS > 0)
			props.put("delivery.timeout.ms", this.deliveryTimeoutMS);

		if (this.batchSize > 0)
			props.put("batch.size", this.batchSize);

		if (this.lingerMS > 0)
			props.put("linger.ms", this.lingerMS);

		if (this.bufferMemory > 0)
			props.put("buffer.memory", this.bufferMemory);

		if (this.keySerializer != null)
			props.put("key.serializer", this.keySerializer);

		if (this.valueSerializer != null)
			props.put("value.serializer", this.valueSerializer);

		producer = new KafkaProducer<>(props);
	}

	// getters and setters
	public void send(String topic, K key, V value) {
		producer.send(new ProducerRecord<K, V>(topic, key, value));
	}

	public void close() {
		producer.close();
	}

	public String getBootstrapServers() {
		return bootstrapServers;
	}

	public void setBootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}

	public String getAcks() {
		return acks;
	}

	public void setAcks(String acks) {
		this.acks = acks;
	}

	public long getDeliveryTimeoutMS() {
		return deliveryTimeoutMS;
	}

	public void setDeliveryTimeoutMS(long deliveryTimeoutMS) {
		this.deliveryTimeoutMS = deliveryTimeoutMS;
	}

	public long getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(long batchSize) {
		this.batchSize = batchSize;
	}

	public long getLingerMS() {
		return lingerMS;
	}

	public void setLingerMS(long lingerMS) {
		this.lingerMS = lingerMS;
	}

	public long getBufferMemory() {
		return bufferMemory;
	}

	public void setBufferMemory(long bufferMemory) {
		this.bufferMemory = bufferMemory;
	}

	public String getKeySerializer() {
		return keySerializer;
	}

	public void setKeySerializer(String keySerializer) {
		this.keySerializer = keySerializer;
	}

	public String getValueSerializer() {
		return valueSerializer;
	}

	public void setValueSerializer(String valueSerializer) {
		this.valueSerializer = valueSerializer;
	}

	public Producer<K, V> getProducer() {
		return producer;
	}

	public void setProducer(Producer<K, V> producer) {
		this.producer = producer;
	}
}
