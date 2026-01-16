package application;
	
import javafx.application.Application;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.GridPane;
import javafx.stage.Stage;

public class RecommendationLetterSimulator extends Application {

    public static void main(String[] args) {
        launch(args);
    }

    private final static String title = "Data Entry";
    @Override
    public void start(Stage primaryStage) {


        //creating interface for data entry
        TextField firstNameField = new TextField();
        TextField lastNameField = new TextField();
        ComboBox<String> genderComboBox = new ComboBox<>();
        genderComboBox.getItems().addAll("Male", "Female");
        DatePicker currentDatePicker = new DatePicker();
        ComboBox<String> programComboBox = new ComboBox<>();
        programComboBox.getItems().addAll("Master of Science(MS)", "Master of business administration(MBA)",
                "Doctor of philosophy(PHD)");
        ComboBox<String> semesterComboBox = new ComboBox<>();
        semesterComboBox.getItems().addAll("Fall", "Spring", "Summer");
        TextField yearField = new TextField();
        ListView<String> coursesListView = new ListView<>();
        coursesListView.getItems().addAll("CS151: Object-Oriented Design", "CS166: Information Security", "CS154: Theory of Computation",
                "CS160: Software Engineering", "CS256: Cryptography", "CS146: Data Structures and Algorithms", "CS152: Programming Languages Paradigm");
        TextField gradeField = new TextField();
        ListView<String> personalCharacteristicsListView = new ListView<>();
        personalCharacteristicsListView.getItems().addAll("very passionate", "very enthusiastic", "punctual", "attentive", "polite");
        ListView<String> academicCharacteristicsListView = new ListView<>();
        academicCharacteristicsListView.getItems().addAll("submitted well-written assignments", "participated in all of my class activities", "worked hard", "was very well prepared for every exam and assignment", "picked up new skills quickly",
                "was able to excel academically at the top of my class");

        //Creating Form Labels
        Label firstNameLabel = new Label("First Name:");
        Label lastNameLabel = new Label("Last Name:");
        Label genderLabel = new Label("Gender:");
        Label targetSchoolLabel = new Label("Target School:");
        Label currentDateLabel = new Label("Current Date:");
        Label programLabel = new Label("Program:");
        Label semesterLabel = new Label("Semester:");
        Label yearLabel = new Label("Year:");
        Label coursesLabel = new Label("Courses:");
        Label gradeLabel = new Label("Grade:");
        Label personalCharacteristicsLabel = new Label("Personal Characteristics:");
        Label academicCharacteristicsLabel = new Label("Academic Characteristics:");

        //Create submit button
        Button submitButton = new Button("Submit");

        //create form layout
        GridPane formLayout = new GridPane();
        formLayout.setPadding(new Insets(10));
        formLayout.setHgap(10);
        formLayout.setVgap(10);
        formLayout.add(firstNameLabel, 0, 0);
        formLayout.add(firstNameField, 1, 0);
        formLayout.add(lastNameLabel, 0, 1);
        formLayout.add(lastNameField, 1, 1);
        formLayout.add(genderLabel, 0, 2);
        formLayout.add(genderComboBox, 1, 2);
        formLayout.add(targetSchoolLabel, 0, 3);
        formLayout.add(new TextField(), 1, 3);
        formLayout.add(currentDateLabel, 0, 4);
        formLayout.add(currentDatePicker, 1, 4);
        formLayout.add(programLabel, 0, 5);
        formLayout.add(programComboBox, 1, 5);
        formLayout.add(semesterLabel, 0, 6);
        formLayout.add(semesterComboBox, 1, 7);
        formLayout.add(yearLabel, 2, 6);
        formLayout.add(yearField, 3, 6);
        formLayout.add(coursesLabel, 0, 7);
        formLayout.add(coursesListView, 1, 7);
        formLayout.add(gradeLabel, 2, 7);
        formLayout.add(gradeField, 3, 5);

        Scene scene = new Scene(formLayout, 1200, 800);
        primaryStage.setScene(scene);
        primaryStage.setTitle(title);
        primaryStage.show();

    }
}
