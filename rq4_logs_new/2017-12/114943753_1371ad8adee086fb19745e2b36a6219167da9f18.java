package com.pauloneto.sampleejb;

import javax.ejb.Schedule;
import javax.ejb.Stateless;

@Stateless
public class EjbScheduler {

	
	@Schedule(minute="*/1", hour="*", dayOfMonth="*", month="*", year="*")
	public void executar() {
		System.out.println("OLHA EU AI KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK");
	}
}