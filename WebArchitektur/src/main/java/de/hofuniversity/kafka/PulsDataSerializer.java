package de.hofuniversity.kafka;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.hofuniversity.general.PulsData;


public class PulsDataSerializer implements Serializer<PulsData> {

	ObjectMapper mapper = new ObjectMapper();



	@Override
	public void close() {
		// nop
	}

	@Override
	public void configure(Map arg0, boolean arg1) {
		// TODO Auto-generated method stub

	}

	@Override
	public byte[] serialize(String arg0, PulsData data) {
		try {
			return mapper.writeValueAsBytes(data);
		} catch (JsonProcessingException ex) {
			throw new SerializationException("Could not transform Object to JSON: " + ex.getMessage(), ex);
		}
	}
}