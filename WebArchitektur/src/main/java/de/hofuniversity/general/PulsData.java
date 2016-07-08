package de.hofuniversity.general;

import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PulsData {
	public final String id;
	public final int bpm;

	@JsonCreator
	public PulsData(@JsonProperty("id") String id, @JsonProperty("bmp") int bpm) {
		this.id = id;
		this.bpm = bpm;
	}
}
