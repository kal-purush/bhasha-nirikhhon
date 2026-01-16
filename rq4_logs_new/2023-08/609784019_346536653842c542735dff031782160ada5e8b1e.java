package application;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import com.calendarfx.model.Entry;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.Pane;
import javafx.scene.text.Text;
import javafx.stage.Stage;

public class DashboardController_old {
	private Stage stage;
	private static Stage calendarStage;
	private Scene scene;
	private static Parent calendarRoot;
	private static CalendarApp myCalendar;
	private String currentUser;
	private Timer timer;
	
	@FXML
	private Pane patientDirectoryDBPane;
	@FXML
	private Pane appointmentsDBPane;
	@FXML
	private Pane patientNotesDBPane;
	@FXML
	private Button viewPatientInfoBtn;
	@FXML
	private Button patientInformationBTN;
	@FXML
	private Text nextAppointment;
	@FXML
	private Text fullName;
	@FXML
	private Text userText;
	@FXML
	private Text phoneNumber;
	@FXML
	private Text pastMedicalConditions;
	@FXML
	private Text progressNotes;
	
	DBConnector dbConnector = new DBConnector();
	String nextA;
	String nextTitle;
	String nextId;
	Boolean nextFullDay;
	LocalDate nextStartDate;
	LocalDate nextEndDate;
	LocalTime nextStartTime;
	LocalTime nextEndTime;
	ZoneId nextZoneId;
	Boolean nextRecurring;
	String nextRRule;
	Boolean nextRecurrence;
	
	@FXML
	public void initialize() throws ClassNotFoundException, SQLException, NullPointerException{
		LocalTime LastEndTime = LocalTime.MAX ; // need a time to compare that is the Max
		dbConnector.initialiseDB();
		
		// grabs the events from the database and inserts to the calendar
		dbConnector.getCalendarEvents(); // this will ensure that the next appointment is displayed 
		
		// get the next appointment 
		System.out.println("Find entries" + CalendarApp.getDoctors().findEntries(LocalDate.now(), LocalDate.MAX, ZoneId.systemDefault()));
		Map<LocalDate, List<Entry<?>>> entry = CalendarApp.getDoctors().findEntries(LocalDate.now(), LocalDate.MAX, ZoneId.systemDefault());
		System.out.println("This is the entry: " + entry);	
		System.out.println("This is the calendar: " + CalendarApp.getDoctors());	
			for (java.util.Map.Entry<LocalDate, List<Entry<?>>> l : entry.entrySet()) {
				System.out.println("This is the list: " + l);
				List<Entry<?>> e =  l.getValue();
				System.out.println("This is entry: " + e);
				nextA = e.get(0).getTitle();
				System.out.println("First entry: " + nextA);
				// this will set the next appointment text 
				if (nextAppointment != null) { // this fixed the bug 
					nextAppointment.setText(nextA);
				}		// this is where it previously breaks because nestAppointment was null
				
				// loop through to add to database
				for (Entry<?> ee: e) {
					nextTitle = ee.getTitle();
					nextId = ee.getId();
					nextFullDay = ee.isFullDay();
					nextStartDate = ee.getStartDate();
					nextEndDate = ee.getEndDate();
					nextStartTime = ee.getStartTime();
					nextEndTime = ee.getEndTime();
					nextZoneId = ee.getZoneId();
					nextRecurring = ee.isRecurring();
					nextRRule = ee.recurrenceRuleProperty().getValue();
					nextRecurrence = ee.isRecurrence();
					
					System.out.println("Entry from loop: " + nextTitle + ", " + nextId + ", " 
					+ nextFullDay + ", " + nextStartDate + ", " + nextEndDate + ", "
					+ nextStartTime + ", " + nextEndTime + "' " + nextZoneId + ", "
					+ nextRecurring + ", " + nextRRule + ", " + nextRecurrence);	
					
					// adds the event to the database
					try {dbConnector.addCalendarEvent(nextTitle, nextId, nextFullDay, nextStartDate, 
							nextEndDate, nextStartTime,	nextEndTime, nextZoneId,
							nextRecurring, nextRRule, nextRecurrence); 
					} catch (SQLIntegrityConstraintViolationException error) {
						System.out.println(error);
						
					}
					
					int value = nextEndTime.compareTo(LocalTime.now()); // ensure that the time is more than the local time for the next appointment 
					int value2 = nextEndTime.compareTo(LastEndTime); // ensure that the current appointment is not later than the next appointment 
					
					if (value > 0 && value2 < 0) { // ensure that the event is more than the local time and less than the next appointment(remember the next appointment must be more than the local time
						if (nextAppointment != null) {
							nextAppointment.setText(nextTitle);
						}
						LastEndTime = nextEndTime;
						// this will display the next patient appointment details
						if (nextA != null && nextAppointment != null) { // there was a bug where it broke if the next appointment was null, fixed by adding nextAppointment
							ResultSet patientDetails = dbConnector.QueryReturnResultsFromPatientName(nextTitle);
							System.out.println("This is the patient details: " + patientDetails);
							if(patientDetails.next()) {
								String name =  patientDetails.getString("FirstName") + " " 
										+ patientDetails.getString("MiddleName") + " " 
										+ patientDetails.getString("LastName");
								fullName.setText(name);
								phoneNumber.setText(patientDetails.getString("Telephone"));
								pastMedicalConditions.setText(patientDetails.getString("PastMedicalConditions"));
								progressNotes.setText(patientDetails.getString("ProgressNotes"));
							}
						}
					}		// this is where it previously breaks because nestAppointment was null
					
					
				}
			}
		System.out.println("After loop: " + nextA);	
		System.out.println("After loop next apppointment: " + nextAppointment);			
		
		// serialize and save the file to desktop, not serializable (only works with Events
//		try {
//			serialize("C:\\Users\\User\\Desktop\\test.ser", entry);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		// de-serialize the file from the desktop, not serializable
//		try {
//			System.out.println("Return deserialize: " + deserialize("C:\\Users\\User\\Desktop\\test.ser"));
//		} catch (ClassNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		//startLoggedInStatusTimer();
	}
	
	public void setUserText(String username) {
	    userText.setText(username);
	    currentUser = username;
	}
	
	private void startLoggedInStatusTimer() {
	    Timer timer = new Timer();
	    timer.schedule(new TimerTask() {
	        @Override
	        public void run() {
	            checkLoggedInStatus(timer);
	        }
	    }, 0, 5000); // Run the task every 5 seconds
	}
	
	private void checkLoggedInStatus(Timer timer) {
	    Platform.runLater(() -> {
	        try {
	            int loggedInStatus = dbConnector.getLoggedInStatus(currentUser);
	            if (loggedInStatus == 0) {
	                // User has been logged out, show alert and provide options to continue or logout
	                timer.cancel(); // Stop the timer after detecting the change to 0

	                Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
	                alert.setTitle("Login Attempt Detected");
	                alert.setHeaderText("Another User Has Attempted to Access This Account:");
	                alert.setContentText("Do you want to continue the session?");
	                dbConnector.setLoggedInStatus(currentUser, 1);
	                ButtonType continueButton = new ButtonType("Continue");
	                ButtonType logoutButton = new ButtonType("Logout");
	                alert.getButtonTypes().setAll(continueButton, logoutButton);

	                Stage alertStage = (Stage) alert.getDialogPane().getScene().getWindow();
	                alertStage.setAlwaysOnTop(true); // Make the alert window stay on top

	                alert.showAndWait().ifPresent(response -> {
	                    if (response == continueButton) {
	                        // User chose to continue, set logged_in status to 1
	                        try {
	                            dbConnector.setLoggedInStatus(currentUser, 1);
	                            startLoggedInStatusTimer(); // Restart the timer
	                        } catch (SQLException e) {
	                            e.printStackTrace();
	                        }
	                    } else if (response == logoutButton) {
	                        // User chose to logout, set logged_in status to 0 and go back to the login screen
	                        try {
	                        	timer.cancel();
	                            dbConnector.setLoggedInStatus(currentUser, 0);
	                            switchToLoginScreen();
	                        } catch (SQLException | IOException e) {
	                            e.printStackTrace();
	                        }
	                    }
	                });
	            }
	        } catch (SQLException e) {
	            e.printStackTrace();
	        }
	    });
	}
    
    @FXML
    private void switchToLoginScreen() throws IOException {
        Parent root = FXMLLoader.load(getClass().getResource("../fxmlScenes/Login.fxml"));
        stage = (Stage) userText.getScene().getWindow();
        scene = new Scene(root);
        stage.setScene(scene);
        stage.show();
    }
	
	@FXML 	
	public void switchToPatientInformation(MouseEvent mouseEvent) throws Exception {
		Parent root = FXMLLoader.load(getClass().getResource("../fxmlScenes/PatientInformation.fxml"));		
		stage = (Stage)((Node)mouseEvent.getSource()).getScene().getWindow();
		scene = new Scene(root);
		stage.setScene(scene);
		stage.show();
	}
	
	@FXML 
	public void switchToPatientDirectory(MouseEvent mouseEvent) throws Exception {
		Parent root = FXMLLoader.load(getClass().getResource("../fxmlScenes/PatientDirectory.fxml"));
		stage = (Stage)((Node)mouseEvent.getSource()).getScene().getWindow();
		scene = new Scene(root);
		stage.setScene(scene);
		stage.show();
	}
	
	@FXML	
	public void switchToTableCreator(MouseEvent mouseEvent) throws IOException {
		Parent root = FXMLLoader.load(getClass().getResource("../fxmlScenes/TableCreator.fxml"));
		stage = (Stage)((Node)mouseEvent.getSource()).getScene().getWindow();
		scene = new Scene(root);
		stage.setScene(scene);
		stage.show();
	}
	
	@FXML	
	public void switchToPatientInfoView(MouseEvent mouseEvent) throws IOException {
		Parent root = FXMLLoader.load(getClass().getResource("../fxmlScenes/PatientInfoView.fxml"));
		stage = (Stage)((Node)mouseEvent.getSource()).getScene().getWindow();
		scene = new Scene(root);
		stage.setScene(scene);
		stage.show();
	}
	
	public void switchToCalendar(MouseEvent mouseEvent) throws Exception {
		if (myCalendar == null) {
			FXMLLoader fxmlLoader = new FXMLLoader(getClass().getResource("../fxmlScenes/Calendar.fxml"));
	        calendarRoot = (Parent) fxmlLoader.load();
			calendarStage = new Stage();
			calendarStage.setScene(new Scene(calendarRoot));
			calendarStage.show();
		
			myCalendar = new CalendarApp();
			myCalendar.start(calendarStage);
			} else calendarStage.show();
	}
	
	@FXML	
	public void highlightPatientDirectoryPane(MouseEvent mouseEvent) throws IOException {
		patientDirectoryDBPane.setStyle("-fx-background-color: #02181f");
	}
	
	@FXML	
	public void highlightPatientDirectoryPaneOnExit(MouseEvent mouseEvent) throws IOException {
		patientDirectoryDBPane.setStyle("-fx-background-color:  #063847");
	}
	
	@FXML	
	public void highlightAppointmentsPaneOnEnter(MouseEvent mouseEvent) throws IOException {
		appointmentsDBPane.setStyle("-fx-background-color: #02181f");
	}
	
	@FXML	
	public void highlightAppointmentsPaneOnExit(MouseEvent mouseEvent) throws IOException {
		appointmentsDBPane.setStyle("-fx-background-color:  #063847");
	}
	
	@FXML	
	public void highlightPatientNotesPaneOnEnter(MouseEvent mouseEvent) throws IOException {
		patientNotesDBPane.setStyle("-fx-background-color: #02181f");
	}
	
	@FXML	
	public void highlightPatientNotesPaneOnExit(MouseEvent mouseEvent) throws IOException {
		patientNotesDBPane.setStyle("-fx-background-color:  #063847");
	}
}