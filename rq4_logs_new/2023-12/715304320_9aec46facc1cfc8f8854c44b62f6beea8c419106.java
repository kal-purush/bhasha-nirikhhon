package com.csis3275.test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

import com.csis3275.model.*;

import org.junit.jupiter.api.Test;

class Patient_ths_01UnitTest {

	@Test
    void testHeightBoundaryValues() {
        
        Patient_ths_01 patient = new Patient_ths_01();
      
        patient.setHeight(145.0);
        double minHeight = patient.getHeight();

        patient.setHeight(250.0);
        double maxHeight = patient.getHeight();

        assertEquals(145.0, minHeight);
        assertEquals(250.0, maxHeight);
    }
	
	 @Test
	 void testWeightBoundaryValues() {
	        
	        Patient_ths_01 patient = new Patient_ths_01();

	        patient.setWeight(50.0);
	        double minWeight = patient.getWeight();

	        patient.setWeight(300.0);
	        double maxWeight = patient.getWeight();

	        assertEquals(50.0, minWeight);
	        assertEquals(300.0, maxWeight);
	    }
	 
	 @Test
	    void testSymptomsListBoundaryValues() {
	        
	        Patient_ths_01 patient = new Patient_ths_01();

	        patient.setSymptoms(Arrays.asList("Headache"));
	        List<String> oneSymptom = patient.getSymptoms();

	        patient.setSymptoms(Arrays.asList("Headache", "Fever"));
	        List<String> twoSymptoms = patient.getSymptoms();

	        assertTrue(oneSymptom.contains("Headache"));
	        assertEquals(1, oneSymptom.size());
	        assertTrue(twoSymptoms.contains("Headache") && twoSymptoms.contains("Fever"));
	        assertEquals(2, twoSymptoms.size());
	    }
}