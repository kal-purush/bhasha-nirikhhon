package org.springframework.samples.petclinic.service;

import java.util.List;

import org.springframework.samples.petclinic.model.Vet;

public interface AppointmentService {
	
	public List<DailyAppointmentSlots> getAvailaibleAppointmentForVet(Vet v, int days);

}