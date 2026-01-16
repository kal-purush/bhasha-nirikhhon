package fhir;

import static org.junit.Assert.assertEquals;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;

import edu.asu.bmi.cda.fhir.*;
import edu.asu.bmi.cda.fhir.CodeableConcept;
import edu.asu.bmi.cda.fhir.Coding;
import edu.asu.bmi.cda.fhir.DateTime;
import edu.asu.bmi.cda.fhir.HumanName;
import edu.asu.bmi.cda.fhir.Identifier;
import edu.asu.bmi.cda.fhir.Patient;
import edu.asu.bmi.cda.fhir.String;

/**
 * @author TS
 *
 */
// -----------------------------------------------------------------
//   Patient.java setX(CodeableConcept) modified from AppWorks and
//	 github CDA591 repo
// -----------------------------------------------------------------

public class patientTest {

	@Test
	    public  void testPatient() throws ParseException{
	        
	        // -----------------------------------------------------------------
	        //   Names(HumanName)
	        // -----------------------------------------------------------------
		        HumanName patientName = new HumanName();
		        String givenName = new String();
		        String familyName = new String();
		        givenName.setValue("Joe");
		        familyName.setValue("Kerr");
		       
		        patientName.setText(givenName);
		        patientName.getGivens().add(givenName);
		        patientName.getFamilies().add(familyName);			
		        //p.getNames().add(patientName);

	        // -----------------------------------------------------------------
	        //   Identifiers(Identifier): Hospital Patient ID
	        // -----------------------------------------------------------------
		        String patientId = new String();
		        patientId.setValue("5912015");
		        Identifier pID = new Identifier();
		        pID.setValue(patientId);
		        //p.getIdentifiers().add(pID);	       
		        
	        // -----------------------------------------------------------------
	        //   BirthDate(DateTime)
	        // -----------------------------------------------------------------
		        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm");
		        DateTime dateTime = new DateTime();
		        Date birthDate = sdf.parse("06/17/2010 04:07");
		        dateTime.setValue(birthDate);	        
		        //p.setBirthDate(dateTime);

			// -----------------------------------------------------------------
	        //   Gender(CodeableConcept)
	        // -----------------------------------------------------------------
				CodeableConcept genderCC = new CodeableConcept();
				Coding genderCoding = new Coding()
				.withDisplay(new String().withValue("MALE"))
				.withCode(new Code().withValue("M"));
				genderCC.getCodings().add(genderCoding);
				//p.setGender(genderCC);
			
			// -----------------------------------------------------------------
	        //   MaritalStatus(CodeableConcept)
	        // -----------------------------------------------------------------
		        CodeableConcept maritalCC = new CodeableConcept();
		        Coding maritalCoding = new Coding()
		        .withDisplay(new String().withValue("Never Married"))
		        .withCode(new Code().withValue("S"));
		        maritalCC.getCodings().add(maritalCoding);
		        //p.setMaritalStatus(maritalCC);
	               
	        // -----------------------------------------------------------------
	        //  New Patient p2: set as contact for Patient p
	        // -----------------------------------------------------------------
		        Patient p2 = new Patient();
		        HumanName patientName2 = new HumanName();
		        String givenName2 = new String();
		        String familyName2 = new String();
		        givenName2.setValue("Calista");
		        familyName2.setValue("Kerr");
		       
		        patientName2.setText(givenName2);
		        patientName2.getGivens().add(givenName2);
		        patientName2.getFamilies().add(familyName2);
		        p2.getNames().add(patientName2);
		        PatientContact pContact = new PatientContact()
		        .withName(patientName2)
		        .withRelationships(new CodeableConcept().withCodings(new Coding().withDisplay(new String().withValue("Emergency"))));
		      	//p.getContacts().add(pContact);
		        
		        
	        // -----------------------------------------------------------------
	        //   Instantiate new Patient p, set values
	        // -----------------------------------------------------------------
				Patient p = new Patient();
		        p.setGender(genderCC);
		        p.setMaritalStatus(maritalCC);
		        p.setBirthDate(dateTime);
		        p.getNames().add(patientName);
		        p.getIdentifiers().add(pID);
		        p.getContacts().add(pContact);
  

	        // -----------------------------------------------------------------
	        //  Test and Print
	        // -----------------------------------------------------------------	        
				assertEquals(p.getNames().get(0).getGivens().get(0).getValue(),"Joe");
				assertEquals(p.getNames().get(0).getFamilies().get(0).getValue(),"Kerr");
				assertEquals(p.getIdentifiers().get(0).getValue().getValue(),"5912015");
		        assertEquals(p.getBirthDate().getValue(),birthDate);		
		        assertEquals(p.getGender().getCodings().get(0).getDisplay().getValue(),"MALE");
				assertEquals(p.getGender().getCodings().get(0).getCode().getValue(),"M");	
				assertEquals(p.getMaritalStatus().getCodings().get(0).getDisplay().getValue(),"Never Married");
		        assertEquals(p.getMaritalStatus().getCodings().get(0).getCode().getValue(),"S");
				assertEquals(p.getContacts().get(0).getName().getGivens().get(0).getValue(),"Calista");	        
				assertEquals(p.getContacts().get(0).getName().getFamilies().get(0).getValue(),"Kerr");	        
				assertEquals(p.getContacts().get(0).getRelationships().get(0).getCodings().get(0).getDisplay().getValue(),"Emergency");
				
				System.out.println("Patient Name: " + p.getNames().get(0).getGivens().get(0).getValue() +" "+ 
								p.getNames().get(0).getFamilies().get(0).getValue());
				System.out.println("Patient ID: " + p.getIdentifiers().get(0).getValue().getValue());
				System.out.println("Birth Date: " + p.getBirthDate().getValue());
				System.out.println("Gender: " + p.getGender().getCodings().get(0).getDisplay().getValue() +
									" ("+ p.getGender().getCodings().get(0).getCode().getValue() +")");
				System.out.println("Marital Status: " + p.getMaritalStatus().getCodings().get(0).getDisplay().getValue() +
									" ("+ p.getMaritalStatus().getCodings().get(0).getCode().getValue()+ ")");
				System.out.println("Patient's Emergency Contact: " + p.getContacts().get(0).getName().getGivens().get(0).getValue() +" "+
									p.getNames().get(0).getFamilies().get(0).getValue());
				
	    }
}