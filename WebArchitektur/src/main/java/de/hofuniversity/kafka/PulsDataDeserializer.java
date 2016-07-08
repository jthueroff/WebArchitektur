package de.hofuniversity.kafka;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.hofuniversity.general.PulsData;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class PulsDataDeserializer implements Deserializer<PulsData> {

	private final ObjectMapper mapper = new ObjectMapper();

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
		// TODO Auto-generated method stub

	}

	@Override
	public PulsData deserialize(String arg0, byte[] bytes) {
		try {
			return mapper.readValue(bytes, PulsData.class);
		} catch (IOException e) {
			throw new SerializationException("Cannot read message.", e);
		}
	}
}