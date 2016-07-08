package de.hofuniversity.kafka;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

import de.hofuniversity.general.PulsData;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class PulsDataConsumer implements Runnable {

	KafkaConsumer<String, PulsData> consumer;
	private Session session;

	public PulsDataConsumer(String zookeeper, String gId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", gId);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", PulsDataDeserializer.class);
		consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("puls-data"));
	}

	public void run() {
		while (true) {
			ConsumerRecords<String, PulsData> records = consumer.poll(200);

			for (ConsumerRecord record : records) {

				evaluatePulsData((PulsData) record.value());
			}
		}
	}

	private void evaluatePulsData(PulsData data) {

		// get user data
		ResultSetFuture future = session.executeAsync("SELECT age,gender FROM user WHERE id = ?",UUID.fromString(data.id));
		
		while (!future.isDone()) {
			;
		}

		ResultSet rs;
		try {
			rs = future.get();
			int age = rs.one().getInt("age");
			int gender = rs.one().getInt("gender");
			
			if(gender == 0)
			{
				evaluateMenPulsData(data,age);
			}
			else
				evaluateWomenPulsData(data,age);
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	

	}
	
	private void evaluateMenPulsData(PulsData data, int age)
	{
		String status="";
		
		if(age > 18 && age < 35)
		{
			if(data.bpm < 65)
				status = "normal";
			else if(data.bpm > 65 && data.bpm <= 81)
				status = "überdurchschnittlich";
			else
				status = "zu hoch";
		}
		else if(age > 35 && age < 45)
		{
			if(data.bpm < 66)
				status = "normal";
			else if(data.bpm >= 66 && data.bpm <= 82)
				status = "überdurchschnittlich";
			else
				status = "zu hoch";
		}
		else if(age > 45 && age < 55)
		{
			if(data.bpm < 67)
				status = "normal";
			else if(data.bpm >= 67 && data.bpm <= 83)
				status = "überdurchschnittlich";
			else
				status = "zu hoch";
		}
		else if(age > 55 && age < 65)
		{
			if(data.bpm < 67)
				status = "normal";
			else if(data.bpm >= 67 && data.bpm <= 81)
				status = "überdurchschnittlich";
			else
				status = "zu hoch";
		}
		else if(age > 65)
		{
			if(data.bpm < 65)
				status = "normal";
			else if(data.bpm >= 65 && data.bpm <= 79)
				status = "überdurchschnittlich";
			else
				status = "zu hoch";
		}
		insertPulsData(data, status);
	}
	
	private void evaluateWomenPulsData(PulsData data, int age)
	{
		String status="";
		
		if(age > 18 && age < 35)
		{
			if(data.bpm < 69)
				status = "normal";
			else if(data.bpm >= 69 && data.bpm <= 84)
				status = "überdurchschnittlich";
			else
				status = "zu hoch";
		}
		else if(age > 35 && age < 45)
		{
			if(data.bpm < 66)
				status = "normal";
			else if(data.bpm >= 66 && data.bpm <= 82)
				status = "überdurchschnittlich";
			else
				status = "zu hoch";
		}
		else if(age > 45 && age < 55)
		{
			if(data.bpm < 69)
				status = "normal";
			else if(data.bpm >= 69 && data.bpm <= 83)
				status = "überdurchschnittlich";
			else
				status = "zu hoch";
		}
		else if(age > 55 && age < 65)
		{
			if(data.bpm < 68)
				status = "normal";
			else if(data.bpm >= 68 && data.bpm <= 83)
				status = "überdurchschnittlich";
			else
				status = "zu hoch";
		}
		else if(age > 65)
		{
			if(data.bpm < 68)
				status = "normal";
			else if(data.bpm >= 68 && data.bpm <= 84)
				status = "überdurchschnittlich";
			else
				status = "zu hoch";
		}
		insertPulsData(data, status);
	}
	
	private void insertPulsData(PulsData data, String status)
	{
		Date ts = new Date();
		session.executeAsync("INSERT INTO pulsdata(ts,userId,bpm,status) VALUES(?,?,?,?)",ts,data.id,data.bpm,status);
	}
}