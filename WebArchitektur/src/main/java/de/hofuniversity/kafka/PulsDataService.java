package de.hofuniversity.kafka;

import javax.annotation.PostConstruct;
import javax.inject.*;

import de.hofuniversity.general.PulsData;

public class PulsDataService {

	@Inject 
	PulsDataProducer producer;
	
	@PostConstruct
	public void pulsDataArrived(PulsData data)
	{
		producer.send(data);
	}
}
