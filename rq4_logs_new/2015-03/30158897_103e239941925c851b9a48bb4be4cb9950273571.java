/**
 * 
 */
package edu.asu.bmi.cda.fhir;

import static org.junit.Assert.*;

import java.text.ParseException;
import org.junit.Test;
import java.text.SimpleDateFormat;
import java.util.Date;

import edu.asu.bmi.cda.fhir.*;


/**
 * @author TS
 *
 */
public class patientTest {

	@Test
	    public  void testPatient() throws ParseException{
	        
	        // -----------------------------------------------------------------
	        //   Patient.java setX(CodeableConcept) modified from AppWorks and
	        //	 github CDA591 repo
	        // -----------------------------------------------------------------
	        
	        // -----------------------------------------------------------------
	        //   Gender(CodeableConcept)
	        // -----------------------------------------------------------------
	        CodeableConcept genderCC = new CodeableConcept();
	        Coding genderCoding = new Coding();
	        genderCoding.setDisplay("MALE");
	        genderCC.getCoding().add(genderCoding);
	        
	        // -----------------------------------------------------------------
	        //   MaritalStatus(CodeableConcept)
	        // -----------------------------------------------------------------
	        CodeableConcept maritalCC = new CodeableConcept();
	        Coding maritalCoding new Coding();
	        maritalCoding.setDisplay("Never Married");
	        maritalCC.getCoding().add(maritalCoding);
	        
	        // -----------------------------------------------------------------
	        //   BirthDate(DateTime)
	        // -----------------------------------------------------------------
	        DateTime dateTime = new DateTime();
	        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm");
	        Date birthDate = sdf.parse("06/17/2010 04:07");
	        dateTime.setValue(birthDate);
	        
	        // -----------------------------------------------------------------
	        //   Names(HumanName)
	        // -----------------------------------------------------------------
	        HumanName patientName = new HumanName();
	        patientName.addFamilySimple("Joe");
	        patientName.addGivenSimple("Kerr");
	        
	        // -----------------------------------------------------------------
	        //   Identifiers(Identifier): Hospital Patient ID
	        // -----------------------------------------------------------------
	        String patientId;
	        patientId.setValue("5912015");
	        Identifier pID = new Identifier();
	        pID.setValue(patientId);
	        
	        // -----------------------------------------------------------------
	        //   Instantiate new Patient p, set values
	        // -----------------------------------------------------------------
	        Patient p = new Patient();
	        p.setGender(genderCC);
	        p.setMaritalStatus(maritalCC);
	        p.setBirthDate(dateTime);
	        p.getName().add(patientName);
	        p.getIdentifiers().add(pID);
	        
	        
	        // -----------------------------------------------------------------
	        //  New Patient
	        // -----------------------------------------------------------------
	        //HumanName patientName2 = new HumanName();
	        //patientName2.addFamilySimple("Calista");
	        //patientName2.addGivenSimple("Kerr");
	        //patientContact patientContact = new PatienContact;
	        
	        //p2 = new Patient();
	        //p2.getName().add(patientName2);
	        //p2.Contacts().add(patientName);
	        
	        
	        assertEquals(p.getGender().getCodings().get(0).getDisplaySimple().getValue(),"MALE");
	        assert(p.contains("Joe"));
	        assert(p.contains("Kerr"));
	        assertEquals(p.getBirthDate().getValue(),"06/17/2010 04:10");		
	        assertEquals(p.getIdentifiers().get(0).getValue().getValue(),"5912015");
	        
	        
	    }
}