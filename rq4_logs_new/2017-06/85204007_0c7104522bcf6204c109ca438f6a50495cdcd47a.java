package Interface;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;

import sample.db.patient.JDBC;
import sample.db.patientJPA.JPA;
import sample.db.pojos.Doctor;
import sample.db.pojos.Medication;

public class MenuDoc {
	public void menu() throws IOException {
        System.out.println(""
            + "1. Insert a doctor \n"
        	+ "2. Insert a doctor schedule\n"
            + "3. Show all doctors\n"
            + "4. Show all the doctors from a field\n"
            + "5. Delete a doctor \n"
            + "6. Search for a doctor \n"
            + "Introduce an option:");
    }
	public void menuDoctor(JDBC j) throws IOException, SQLException {
		//JDBC jd = new JDBC();
		
		MenuDoc menu=new MenuDoc();
		Doctor d=null;
        BufferedReader consola = new BufferedReader(new InputStreamReader(System.in));
        int opcion =0;
        menu.menu();
        opcion=Integer.parseInt(consola.readLine());
        switch (opcion) {
            case 1:
            	String nam,sur,field;
            	System.out.println("Insert the doctor name: ");
            	nam=consola.readLine();
            	System.out.println("Insert tha doctor surname:");
            	sur=consola.readLine();
            	System.out.println("Insert the doctor field: ");
            	field=consola.readLine();
            	d=new Doctor(nam, sur,field);
            	j.insertDoc(d);
            	break;
            case 2:
            	System.out.println("Choose a doctor and a time available: ");
            	System.out.println(j.selectD());
            	System.out.println(j.selectSh());
            	System.out.println("Enter the doctor id: ");
            	int id1=Integer.parseInt(consola.readLine());
            	System.out.println("Enter the schedule id: ");
            	int id2=Integer.parseInt(consola.readLine());
            	j.assignDoctorSchedule(id1, id2);
            	break;
            case 3:
            	System.out.println(j.selectD());
                break;
            case 4:
            	System.out.println("Enter the field: ");
            	String f=consola.readLine();
            	System.out.println(j.selectDbyfield(f));
            	break;
            case 5:
            	System.out.println("Enter the doctor name: ");
            	String nm=consola.readLine();
            	System.out.println("Enter the doctor surname: ");
            	String sr=consola.readLine();
            	j.deleteDoctor(nm, sr);
            	break;
            case 6:
            	System.out.println("Enter the doctor name: ");
            	String _nm=consola.readLine();
            	System.out.println("Enter the doctor surname: ");
            	String _sr=consola.readLine();
            	d=j.searchDoc(_nm, _sr);
            	System.out.println(d);
            	break;

        }
	}
}
