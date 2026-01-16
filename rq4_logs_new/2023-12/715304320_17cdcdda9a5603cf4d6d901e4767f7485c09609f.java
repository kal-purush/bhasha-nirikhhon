package com.csis3275.test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Optional;

import com.csis3275.model.*;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.csis3275.model.PatientRepository_ths_01;

class PatientIntegrationTest {

	 @Autowired
	    private PatientRepository_ths_01 patientRepository;

	    @Test
	    void testCRUDOperations() {
	        // Create
	        Patient_ths_01 newPatient = new Patient_ths_01( null,"John", "Doe", "john@example.com", 170.0, 70.0, null);
	        Patient_ths_01 savedPatient = patientRepository.save(newPatient);
	        assertNotNull(savedPatient.getId());

	        // Read
	        Optional<Patient_ths_01> readPatient = patientRepository.findById(savedPatient.getId());
	        assertTrue(readPatient.isPresent());
	        assertEquals("John", readPatient.get().getFirstName());

	        // Update
	        readPatient.ifPresent(patient -> {
	            patient.setFirstName("UpdatedJohn");
	            patientRepository.save(patient);
	        });

	        Optional<Patient_ths_01> updatedPatient = patientRepository.findById(savedPatient.getId());
	        assertTrue(updatedPatient.isPresent());
	        assertEquals("UpdatedJohn", updatedPatient.get().getFirstName());

	        // Delete
	        patientRepository.deleteById(savedPatient.getId());
	        Optional<Patient_ths_01> deletedPatient = patientRepository.findById(savedPatient.getId());
	        assertFalse(deletedPatient.isPresent());
	    }

	    @Test
	    void testSearchPatients() {
	        
	        List<Patient_ths_01> searchResults = patientRepository.search("John");
	        assertFalse(searchResults.isEmpty());
	        
	    }

}