import com.healthcare.model.Appointment;
import com.healthcare.model.Doctor;
import com.healthcare.model.Patient;
import com.healthcare.repository.AppointmentRepositoryImpl;
import com.healthcare.repository.DoctorRepositoryImpl;
import com.healthcare.repository.PatientRepositoryImpl;
import com.healthcare.service.AppointmentService;
import com.healthcare.service.DoctorService;
import com.healthcare.service.PatientService;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class AppointmentServiceTest {
    private AppointmentService appointmentService;
    private DoctorService doctorService;
    private PatientService patientService;
    private SessionFactory sessionFactory;
    private Session session;
    private Transaction transaction;

    @BeforeEach
    public void setUp(){
        sessionFactory = new Configuration().configure("hibernate-test.cfg.xml").buildSessionFactory();
        session = sessionFactory.openSession();
        transaction = session.beginTransaction();

        AppointmentRepositoryImpl appointmentRepository = new AppointmentRepositoryImpl(sessionFactory);
        DoctorRepositoryImpl doctorRepository = new DoctorRepositoryImpl(sessionFactory);
        PatientRepositoryImpl patientRepository = new PatientRepositoryImpl(sessionFactory);

        doctorService = new DoctorService(doctorRepository);
        patientService = new PatientService(patientRepository);
        appointmentService = new AppointmentService(appointmentRepository, doctorRepository, patientRepository);
    }
    @AfterEach
    public void tearDown(){
        if(transaction !=null){
            transaction.rollback();
        }
        if(session !=null){
            session.close();
        }
        if(sessionFactory !=null){
            sessionFactory.close();
        }
    }

    @Test
    public void testCreateAppointment(){
        Patient patient = new Patient();
        patient.setFirstName("John");
        patient.setLastName("Doe");
        patientService.createPatient(patient);

        Doctor doctor = new Doctor();
        doctor.setFirstName("Jane");
        doctor.setLastName("Smith");
        doctorService.createDoctor(doctor);

        Appointment appointment = new Appointment();
        appointment.setPatient(patient);
        appointment.setDoctor(doctor);
        appointment.setAppointmentDate("2024-09-01");
        appointment.setNotes("Annual checkup");

        appointmentService.createAppointment(appointment);

        Appointment fetchedAppointment = appointmentService.getAppointmentById(appointment.getAppointmentId());
        assertNotNull(fetchedAppointment,"appointment should exist");
        assertEquals(appointment.getAppointmentId(), fetchedAppointment.getAppointmentId());
    }
    @Test
    public void testGetAppointmentById(){
        // Arrange: Create and persist a new appointment
        Patient patient = new Patient();
        patient.setFirstName("John");
        patient.setLastName("Doe");
        patientService.createPatient(patient);

        Doctor doctor = new Doctor();
        doctor.setFirstName("Jane");
        doctor.setLastName("Smith");
        doctorService.createDoctor(doctor);

        Appointment appointment = new Appointment();
        appointment.setPatient(patient);
        appointment.setDoctor(doctor);
        appointment.setAppointmentDate("2024-09-01");
        appointment.setNotes("Routine checkup");

        appointmentService.createAppointment(appointment);

        // Act: Retrieve the created appointment by ID
        Appointment fetchedAppointment = appointmentService.getAppointmentById(appointment.getAppointmentId());

        // Assert: Verify the appointment exists and matches expected data
        assertNotNull(fetchedAppointment,"appointment should exist in the database");
        assertEquals(appointment.getAppointmentId(), fetchedAppointment.getAppointmentId());
        assertEquals("Routine checkup", fetchedAppointment.getNotes());
    }

    @Test
    public void testGetAllAppointments() {
        // Arrange: Create multiple appointments
        Patient patient1 = new Patient();
        patient1.setFirstName("Alice");
        patient1.setLastName("Johnson");
        patientService.createPatient(patient1);

        Doctor doctor1 = new Doctor();
        doctor1.setFirstName("Bob");
        doctor1.setLastName("Smith");
        doctorService.createDoctor(doctor1);

        Appointment appointment1 = new Appointment();
        appointment1.setPatient(patient1);
        appointment1.setDoctor(doctor1);
        appointment1.setAppointmentDate("2024-10-01");
        appointment1.setNotes("Dental Checkup");
        appointmentService.createAppointment(appointment1);

        Appointment appointment2 = new Appointment();
        appointment2.setPatient(patient1);
        appointment2.setDoctor(doctor1);
        appointment2.setAppointmentDate("2024-11-01");
        appointment2.setNotes("Follow-up");
        appointmentService.createAppointment(appointment2);

        // Act: Retrieve all appointments
        List<Appointment> appointments = appointmentService.getAllAppointments();

        // Assert: Check that appointments were retrieved and match expectations
        assertNotNull(appointments);
        assertTrue(appointments.size() >=2, "There should be at least two appointments in the database.");
    }

    @Test
    public void testUpdateAppointment() {
        Patient patient = new Patient();
        patient.setFirstName("John");
        patient.setLastName("Doe");
        patientService.createPatient(patient);

        Doctor doctor = new Doctor();
        doctor.setFirstName("Jane");
        doctor.setLastName("Smith");
        doctorService.createDoctor(doctor);

        Appointment appointment = new Appointment();
        appointment.setPatient(patient);
        appointment.setDoctor(doctor);
        appointment.setAppointmentDate("2024-09-02");
        appointment.setNotes("Initial notes");

        appointmentService.createAppointment(appointment);
        appointment.setNotes("Updated notes");
        appointmentService.updateAppointment(appointment);

        Appointment updatedAppointment = appointmentService.getAppointmentById(appointment.getAppointmentId());
        assertEquals("Updated notes", updatedAppointment.getNotes());
    }

    @Test
    public void testDeleteAppointment() {
        Patient patient = new Patient();
        patient.setFirstName("John");
        patient.setLastName("Doe");
        patientService.createPatient(patient);

        Doctor doctor = new Doctor();
        doctor.setFirstName("Jane");
        doctor.setLastName("Smith");
        doctorService.createDoctor(doctor);

        Appointment appointment = new Appointment();
        appointment.setPatient(patient);
        appointment.setDoctor(doctor);
        appointment.setAppointmentDate("2024-09-03");
        appointment.setNotes("To be deleted");

        appointmentService.createAppointment(appointment);
        int id = appointment.getAppointmentId();
        appointmentService.deleteAppointmentById(id);

        assertNull(appointmentService.getAppointmentById(id));
        //appointmentService.getAppointmentById(id)
        //This method is expected to retrieve an appointment by its ID. If the appointment does not exist (e.g., it has been deleted), it should return null.
    }

    @ParameterizedTest
    @ValueSource(strings = {"2024-09-01", "2024-09-02", "2024-09-03", "2024-09-04"})
    public void testCreateAppointmentWithDifferentDates(String date) {
        Appointment appointment = new Appointment();
        appointment.setAppointmentDate(date);
        appointment.setNotes("Checkup");

        appointmentService.createAppointment(appointment);
        assertNotNull(appointment.getAppointmentId());
        assertEquals(date, appointment.getAppointmentDate());
    }



}