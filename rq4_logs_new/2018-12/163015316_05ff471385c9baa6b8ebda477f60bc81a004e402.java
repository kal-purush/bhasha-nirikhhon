package doctorappointmentscheduler.client;

import java.util.ArrayList;
import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;
import doctorappointmentscheduler.shared.Appointment;
import doctorappointmentscheduler.shared.AppointmentInfo;
import doctorappointmentscheduler.shared.MedicalReport;
import doctorappointmentscheduler.shared.Patient;
import doctorappointmentscheduler.shared.PatientRecord;
import doctorappointmentscheduler.shared.User;
import doctorappointmentscheduler.shared.UserType;

/*Service for database connection */

@RemoteServiceRelativePath("dbconnectionservice")
public interface DBConnection extends RemoteService {
	public User authenticateUser(String email, String pass); //verifies if the User is in the database
	public String createUser(UserType usertype, String password); //creates a user and store in database
	public Patient getPatient(String email); //gets a Patient from the database based on an email supplied
	public String addReport(MedicalReport medicalReport, Appointment appointment, String email); //add a medical report to database
	public MedicalReport getMedicalReport(String email, int appNum); //gets a Medical Report from the database based on an email and appointment number supplied
	public ArrayList<AppointmentInfo> getAppointmentsFromDateOnwards(String date); //gets all appointments from database after a certain date 
	public ArrayList<PatientRecord> getAllPatientsRecords(); //gets all Patient Records in the database
}  