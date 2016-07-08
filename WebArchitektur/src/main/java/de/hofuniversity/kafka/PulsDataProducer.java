package de.hofuniversity.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import de.hofuniversity.general.PulsData;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * This producer will send a bunch of messages to topic "fast-messages". Every
 * so often, it will send a message to "slow-messages". This shows how messages
 * can be sent to multiple topics. On the receiving end, we will see both kinds
 * of messages but will also see how the two topics aren't really synchronized.
 */
public class PulsDataProducer {

	private KafkaProducer<String,PulsData> producer;
	private final String topic="pulsdata";
	private final String broker = "localhost:9092";
	
	public PulsDataProducer() {
		
		
	  Properties config = new Properties();
	  config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
	  config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
	  config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,PulsDataSerializer.class.getName());
	  this.producer = new KafkaProducer<String,PulsData>(config);
  }

	public Future send(PulsData data) {
		
	    ProducerRecord<String, PulsData> record = new ProducerRecord<> (topic, data.id, data);
	    return this.producer.send(record);
  }

}
