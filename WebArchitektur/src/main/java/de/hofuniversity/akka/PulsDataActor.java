package de.hofuniversity.akka;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

import akka.actor.UntypedActor;
import de.hofuniversity.general.PulsData;

public class PulsDataActor extends UntypedActor {
	
	private Session session;
	
	@Override
	public void onReceive(final Object message) {

		if (message instanceof PulsData) {
			evaluatePulsData((PulsData) message);
		} else {
			unhandled(message);
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