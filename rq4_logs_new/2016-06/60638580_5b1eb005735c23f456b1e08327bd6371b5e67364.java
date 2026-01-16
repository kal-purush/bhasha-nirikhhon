package com.frracm.asclepio.repository;

import java.util.Collection;

import javax.annotation.Resource;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

import com.frracm.asclepio.model.Patient;

@Repository
public class PatientRepository {

	@Resource
	private MongoTemplate template;

	public Collection<Patient> getAll() {
		return template.findAll(Patient.class);
	}

	public void saveOrUpdate(Patient patient) {
		template.save(patient);
	}

	public void remove(Long numeroHistoriaClinica) {
		Patient patient = new Patient();
		patient.setNumeroHistoriaClinica(numeroHistoriaClinica);
		template.remove(patient);
	}
}