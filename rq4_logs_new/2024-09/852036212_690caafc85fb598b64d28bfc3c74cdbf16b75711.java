import java.util.*;

class User {
    private String username;
    private String password;
    private String role; // "patient" or "admin"

    public User(String username, String password, String role) {
        this.username = username;
        this.password = password;
        this.role = role;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getRole() {
        return role;
    }
}

class Patient {
    private String name;
    private String history;

    public Patient(String name) {
        this.name = name;
        this.history = "";
    }

    public String getName() {
        return name;
    }

    public String getHistory() {
        return history;
    }

    public void updateHistory(String newHistory) {
        this.history += newHistory + "\n";
    }
}

class Appointment {
    private String patientName;
    private Date date;

    public Appointment(String patientName, Date date) {
        this.patientName = patientName;
        this.date = date;
    }

    public String getPatientName() {
        return patientName;
    }

    public Date getDate() {
        return date;
    }
}

class PatientManagementApp {
    private Map<String, User> users = new HashMap<>();
    private Map<String, Patient> patients = new HashMap<>();
    private List<Appointment> appointments = new ArrayList<>();

    public void registerUser(String username, String password, String role) {
        users.put(username, new User(username, password, role));
    }

    public boolean loginUser(String username, String password) {
        User user = users.get(username);
        return user != null && user.getPassword().equals(password);
    }

    public void addPatient(String username, String patientName) {
        if (users.containsKey(username) && users.get(username).getRole().equals("admin")) {
            patients.put(patientName, new Patient(patientName));
        }
    }

    public void scheduleAppointment(String patientName, Date date) {
        appointments.add(new Appointment(patientName, date));
    }

    public void updatePatientHistory(String patientName, String newHistory) {
        Patient patient = patients.get(patientName);
        if (patient != null) {
            patient.updateHistory(newHistory);
        }
    }

    public void displayAppointments() {
        for (Appointment appointment : appointments) {
            System.out.println("Appointment for " + appointment.getPatientName() + " on " + appointment.getDate());
        }
    }

    public void displayPatientHistory(String patientName) {
        Patient patient = patients.get(patientName);
        if (patient != null) {
            System.out.println("History for " + patient.getName() + ":");
            System.out.println(patient.getHistory());
        }
    }

    public static void main(String[] args) {
        PatientManagementApp app = new PatientManagementApp();
        app.registerUser("admin", "admin123", "admin");
        app.registerUser("patient1", "pass123", "patient");

        if (app.loginUser("admin", "admin123")) {
            app.addPatient("admin", "John Doe");
            app.scheduleAppointment("John Doe", new Date());
            app.updatePatientHistory("John Doe", "Initial consultation completed.");
            app.displayAppointments();
            app.displayPatientHistory("John Doe");
        }
    }
}

