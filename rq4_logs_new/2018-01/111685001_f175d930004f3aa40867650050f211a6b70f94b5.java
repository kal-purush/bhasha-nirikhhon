package SEProject;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import com.itextpdf.text.DocumentException;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import javafx.application.Application;
import static javafx.application.Application.launch;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.RadioButton;
import javafx.scene.control.TextField;
import javafx.scene.control.Label;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.Pane;
import javafx.scene.layout.VBox;
import javafx.scene.layout.HBox;
import javafx.stage.Stage;
import javafx.geometry.Pos;
import javafx.geometry.VPos;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.MenuItem;
import javafx.scene.control.PasswordField;
import javafx.scene.control.ToggleGroup;
import javafx.scene.text.Font;
import javafx.scene.control.TextArea;
import javafx.scene.input.MouseEvent;
import javafx.stage.FileChooser;
import javafx.scene.text.TextAlignment;

/**
 *
 * @author stefan
 */
public class SimpleLearnerGUI extends Application {

    SqlLogik sql;

    public SimpleLearnerGUI() {
        sql = new SqlLogik();
    }

    @Override
    public void start(Stage primaryStage) {
        // lade CSS-Datei(-en)  //Z.511
        loadStyleSheets();
        // initialisiere AnmeldungPane
        buildLoginPane();
        setBtnLogin(mainStage);
        scene.setRoot(getLoginPane());

        primaryStage = mainStage;
        primaryStage.setTitle("SimpleTest - Anmeldung");
        primaryStage.setScene(scene);
        primaryStage.show();
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        launch(args);
    }

// AnmeldungPane
    TextField loginName = new TextField();
    PasswordField loginPassword = new PasswordField();
    Button btnLogin = new Button("Einloggen");
    BorderPane loginPane = new BorderPane();
    BorderPane loginWindow = new BorderPane();
    VBox loginContainer = new VBox();
    boolean isTeacher;
    String currentUser = null;

    String gName = null;

    String getGName() {
        return gName;
    }

    void setGName(String gName) {
        this.gName = gName;
    }

    boolean isTeacher() {
        return isTeacher;
    }

    private void buildLoginPane() {
        Label loginLabel = new Label("Anmeldung");
        loginLabel.setId("loginLabel");
        loginLabel.setAlignment(Pos.CENTER);
        loginLabel.setTextAlignment(TextAlignment.CENTER);

        Label nameLabel = new Label("Name");
        Label passLabel = new Label("Passwort");
        nameLabel.setId("nameLabel");
        passLabel.setId("passLabel");

        loginName.setId("nameTextField");
        loginName.setPrefHeight(35);
        loginName.setMaxWidth(235);

        loginPassword.setId("passTextField");
        loginPassword.setMaxWidth(235);
        loginPassword.setPrefHeight(35);

        btnLogin.setId("btnLogin");
        btnLogin.setAlignment(Pos.CENTER);
        btnLogin.setTextAlignment(TextAlignment.CENTER);

        BorderPane tempMain = new BorderPane();

        BorderPane temp = new BorderPane();

        BorderPane loginTop = new BorderPane();
        loginTop.setPrefHeight(20);
        loginTop.setCenter(loginLabel);

        loginContainer = new VBox(8);
        loginContainer.setPadding(new Insets(10, 50, 50, 50));
        loginContainer.setId("loginContainer");
        loginContainer.getChildren().addAll(loginLabel, nameLabel, loginName, passLabel, loginPassword, btnLogin);
        loginContainer.setMinSize(300, 250);
        loginContainer.setMaxSize(300, 250);

        temp.setTop(loginTop);
        temp.setCenter(loginContainer);

        Pane a1 = new Pane();
        a1.setPrefHeight((scene.getHeight() /* -btnLogout.getHeight() */) / 2.5);

        Pane a2 = new Pane();
        a2.setPrefHeight((scene.getHeight() /* -btnLogout.getHeight() */) / 2.5);

        Pane b1 = new Pane();
        b1.setPrefWidth(150);

        Pane b2 = new Pane();
        b2.setPrefWidth(150);

        tempMain.setTop(a1);
        tempMain.setBottom(a2);
        tempMain.setLeft(b1);
        tempMain.setRight(b2);
        tempMain.setCenter(temp);
        loginPane = tempMain;
    }

    void setBtnLogin(Stage tempStage) {
        btnLogin.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent e) {
                System.out.println("------------------------------");
                System.out.println("Benutzer wird eingeloggt");
                System.out.println("    Name: " + loginName.getText());
                System.out.println("    Passwort: " + loginPassword.getText());
                //System.out.println("------------------------------");   
                boolean[] check = new boolean[2];
                try {
                    check = sql.checkLogin(loginName.getText(), loginPassword.getText());
                    System.out.println(check[1]);
                    if (check[0] == true) {
                        if (check[1] == true) {//LehrerPane erstellen
                            isTeacher = check[1];
                            System.out.println("Anmeldung Lehrer >>> true");
                        } else if (check[1] == false) { //SchÃ¼lerPane erstellen
                            isTeacher = check[1];
                            System.out.println("Anmeldung Lehrer >>> false");
                        }

                        currentUser = sql.getCurrentUser();
                        System.out.println(currentUser);

                        System.out.println("    Lehrer: " + isTeacher());
                        hString = "Kategorie";
                        scene.setRoot(getMainContainer());
                        tempStage.setScene(scene);
                        tempStage.setTitle("SimpleLearner - Kategorie");
                        tempStage.show();
                        // initialisere MeldungPane
                        // initialisiere AufgabenPane
                        setBtnConfirmAnswer();
                        setBtnNextTask(mainStage);
                        setBtnConfirmQuiz(mainStage);
                        buildTaskPane();
                        setBtnBlockName();
                        setBtnBack(mainStage);
                        setBtnNewQuestionText();
                        setVBoxTaskContainer();
                        setBtnLoadAnswerChoice();
                        //initialisiere mainContainer  
                        setTopMenu();
                        //setHauptLeft(); setHauptRight(); setHauptBottom(); //zurzeit nicht beÃ¶tigt
                        setMainContainer();
                        //changeHauptCenter(getAufgabenPane()); //Funktion auskommentiert (Z.214)
                        //setScene(getMainContainer());
                        buildMainPane();
                        fillKategorie();
                    } else {
                        System.out.println("Falsche Eingabe");
                    }
                } catch (SQLException exc) {
                    System.out.println(exc.getMessage());
                }

                System.out.println("------------------------------");
            }
        });
    }

    String getName() {
        return loginName.getText();
    }

    String getPasswort() {
        return loginPassword.getText();
    }

    BorderPane getLoginPane() {
        return loginPane;
    }

// mainContainer
    Label labelDirectory = new Label("");
    TextField filter = new TextField();
    BorderPane menuContainer = new BorderPane();
    Pane hauptLeft = new Pane();
    BorderPane hauptRight = new BorderPane();
    BorderPane hauptBottom = new BorderPane();
    BorderPane hauptCenter = new BorderPane();
    VBox centerListe = new VBox();
    Button btnNeuesElement = new Button("Neues Element");
    BorderPane mainContainer = new BorderPane();

    //SpeicherStrings
    String kategorieString = null;
    String modulString = null;
    String hString = null;

    void setTopMenu() {
        menuContainer.setId("mainTopMenu");
        menuContainer.setPrefHeight(50);
        
        labelDirectory.setId("labelDirectory");
        
        filter.setPromptText("Filter");
        filter.setPrefHeight(30);
        filter.setFocusTraversable(false);
  
        filter.setOnKeyReleased(e -> {
            if (hString.equals("Kategorie")) {
                fillKategorie();
            } else if (hString.equals("Modul")) {
                fillModul(kategorieString);
            } else if (hString.equals("Verzeichnis")) {
                fillVerzeich();
            }
        });
        Button btnLogout = new Button("Abmelden");
        btnLogout.setId("btnLogout");
        
        Button btnBack = new Button("ZurÃ¼ck");
        btnBack.setId("btnBack");
        btnBack.setAlignment(Pos.CENTER);
        HBox hBoxRight = new HBox(8);
        hBoxRight.getChildren().addAll(filter, btnLogout);
        menuContainer.setLeft(btnBack);
        menuContainer.setCenter(labelDirectory);
        menuContainer.setRight(hBoxRight);
        menuContainer.setMargin(hBoxRight, new Insets(10, 10, 10, 10));
        menuContainer.setMargin(btnBack, new Insets(10, 10, 10, 10));
        btnLogout.setOnAction((ActionEvent e) -> {
            //DislogFenster fÃ¼r Namenseingabe
            //-> Erstellung einer neuen Aufgabe
            System.out.println("-----------------");
            System.out.println("Abmeldung startet");
            System.out.println("-----------------");

            loginName.setText("");
            loginPassword.setText("");
            scene.setRoot(getLoginPane());

            Stage tempStage = mainStage;
            tempStage.setTitle("SimpleTest - Anmeldung");
            tempStage.setScene(scene);
            tempStage.show();
        });
        btnBack.setOnAction((ActionEvent e) -> {
            if (hString.equals("Modul")) {
                fillKategorie();
            } else if (hString.equals("Verzeichnis")) {
                fillModul(kategorieString);
                hString = "Modul";
            } else {
                System.out.println("ELSE " + hString);
            }

        });
    }
    
    void setMainContainer() {
        hauptCenter.setId("hauptCenter");
        centerListe.setSpacing(10);
        hauptCenter.setCenter(centerListe);
    }

    class BtnNewElement {

        Button btnNewElement;

        BtnNewElement(String input) {
            btnNewElement = new Button();
            if (input.equals("Modul")) {
                btnNewElement.setText("Neue Kategorie");
            }
            if (input.equals("Verzeich")) {
                btnNewElement.setText("Neuer Aufgabenblock");
            }
            setBtnNewElement(input);
        }

        void setBtnNewElement(String input) {
            btnNewElement.setPrefWidth(scene.getWidth());
            btnNewElement.setId("btnNewElement");
            btnNewElement.setOnAction(new EventHandler<ActionEvent>() {
                @Override
                public void handle(ActionEvent e) {
                    //DislogFenster fÃ¼r Namenseingabe
                    //-> Erstellung einer neuen Aufgabe

                    System.out.println(scene.getWidth());

                    if (input.equals("Modul")) {
                        //fillModul(kategorieString);
                        Stage tempStage = new Stage();
                        tempStage.setTitle("Neue Kategorie");

                        BorderPane borderPane = new BorderPane();
                        TextField textFieldNewCategory = new TextField();
                        textFieldNewCategory.setPromptText("Hier Kategorienamen eintragen");
                        Button btnCancelCategory = new Button("Abbrechen");
                        Button btnConfirmCategory = new Button("Bestaetigen");

                        btnCancelCategory.setOnAction(e2 -> {
                            tempStage.close();
                        });
                        btnConfirmCategory.setOnAction(e3 -> {
                            try {
                                sql.createKategorie(textFieldNewCategory.getText(), kategorieString);
                                fillModul(kategorieString);
                            } catch (SQLException exc) {
                                System.out.println(exc.getMessage());
                            }
                            tempStage.close();
                        });
                        borderPane.setTop(textFieldNewCategory);
                        borderPane.setLeft(btnCancelCategory);
                        borderPane.setRight(btnConfirmCategory);

                        Scene tempScene = new Scene(borderPane, 300, 300);

                        tempStage.setScene(tempScene);
                        tempStage.show();
                    } else if (input.equals("Verzeich")) {
                        //fillVerzeich();
                        Stage tempStage = new Stage();
                        tempStage.setTitle("Neuer Block");

                        BorderPane borderPane = new BorderPane();
                        TextField textFieldNewQuiz = new TextField();
                        textFieldNewQuiz.setPromptText("Hier Blocknamen eintragen");
                        Button btnCancelQuiz = new Button("Abbrechen");
                        Button btnConfirmNewQuiz = new Button("Bestaetigen");

                        btnCancelQuiz.setOnAction(e2 -> {
                            tempStage.close();
                        });
                        btnConfirmNewQuiz.setOnAction(e3 -> {
                            try {
                                sql.createBlock(textFieldNewQuiz.getText(), loginName.getText(), modulString);
                                fillVerzeich();
                            } catch (SQLException exc) {
                                System.out.println(exc.getMessage());
                            }
                            tempStage.close();
                        });
                        borderPane.setTop(textFieldNewQuiz);
                        borderPane.setLeft(btnCancelQuiz);
                        borderPane.setRight(btnConfirmNewQuiz);

                        Scene tempScene = new Scene(borderPane, 300, 300);

                        tempStage.setScene(tempScene);
                        tempStage.show();
                    }

                    //fillVerzeich();
                    System.out.println("Neues Element wird erstellt");
                }
            });
        }

        Button getBtnNeuesElement() {
            return btnNewElement;
        }
    }

    void buildMainPane() {
        mainContainer.setTop(menuContainer);
        mainContainer.setLeft(hauptLeft);
        mainContainer.setRight(hauptRight);
        mainContainer.setBottom(hauptBottom);
        mainContainer.setCenter(hauptCenter);

        fillKategorie();
    }

    void fillKategorie() {
        //tempStage.setTitle("SimpleLearner - Kategorienverzeichnis");

        // Liste leeren
        centerListe.getChildren().setAll();

        // Liste fÃ¼llen
        if (isTeacher) {
            if (filter.getText().isEmpty()) {
                try {
                    sql.loadFaecher(getName());
                } catch (SQLException exc) {
                    System.out.println(exc.getMessage());
                }
            } else {
                try {
                    sql.loadFilteredFaecher(getName(), filter.getText());
                } catch (SQLException exc) {
                    System.out.println(exc.getMessage());
                }
            }
            for (int i = 0; i < sql.faecher.size(); i++) {
                centerListe.getChildren().add(new SubjectButton(sql.faecher.get(i), i).getKategorieButton());
            }
        } else {
            if (this.filter.getText().isEmpty()) {
                try {
                    sql.loadFaecher();
                } catch (SQLException exc) {
                    System.out.println(exc.getMessage());
                }
            } else {
                try {
                    sql.loadFilteredFaecher(filter.getText());
                } catch (SQLException exc) {
                    System.out.println(exc.getMessage());
                }
            }
            for (int i = 0; i < sql.faecher.size(); i++) {
                centerListe.getChildren().add(new SubjectButton(sql.faecher.get(i), i).getKategorieButton());
            }
        }

        //centerListe.getChildren().add(new SubjectButton("Kategorie 0", 0).getKategorieButton());
        //centerListe.getChildren().add(new BtnNewElement("Kategorie").getBtnNeuesElement());
    }

    void fillVerzeich() { //ParameterÃ¼bergabe fÃ¼r Anzahl der Aufgaben
        // -> Anzahl aus Datenbank
        // Liste leeren (Liste soll sich neu fÃ¼llen, nicht erweitern)

        centerListe.getChildren().setAll();

        if (isTeacher) {
            if (filter.getText().isEmpty()) {
                try {
                    sql.loadBloeckeLehrer(getGName(), getName());
                } catch (SQLException exc) {
                    System.out.println(exc.getMessage());
                }
            } else {
                try {
                    sql.loadFilteredBloeckeLehrer(getGName(), getName(), filter.getText());
                } catch (SQLException exc) {
                    System.out.println(exc.getMessage());
                }
            }
            for (int i = 0; i < sql.aufgabenbloecke.size(); i++) {
                centerListe.getChildren().add(new QuizButton(sql.aufgabenbloecke.get(i), i).getQuizButton()); // ersetze ("SimpleLearnerGUI "+i) mit Aufgabenname
            }
            centerListe.getChildren().add(new BtnNewElement("Verzeich").getBtnNeuesElement());
        } else {
            if (filter.getText().isEmpty()) {
                try {
                    sql.loadBloeckeSchueler(getGName(), getName());
                } catch (SQLException exc) {
                    System.out.println(exc.getMessage());
                }
            } else {
                try {
                    sql.loadFilteredBloeckeSchueler(getGName(), getName(), filter.getText());
                } catch (SQLException exc) {
                    System.out.println(exc.getMessage());
                }
            }
            for (int i = 0; i < sql.aufgabenbloecke.size(); i++) {
                centerListe.getChildren().add(new QuizButton(sql.aufgabenbloecke.get(i), i).getQuizButton()); // ersetze ("SimpleLearnerGUI "+i) mit Aufgabenname
            }
        }

    }

    void fillModul(String kategorieString) { //ParameterÃ¼bergabe fÃ¼r Anzahl der Aufgaben
        // -> Anzahl aus Datenbank
        // Liste leeren (Liste soll sich neu fÃ¼llen, nicht erweitern)

        centerListe.getChildren().setAll();

        if (filter.getText().isEmpty()) {
            try {
                sql.loadKategorien(kategorieString);
                System.out.println(getGName() + "adsgonj " + kategorieString);
            } catch (SQLException exc) {
                System.out.println(exc.getMessage());
            }
        } else {
            try {
                sql.loadFilteredKategorien(kategorieString, filter.getText());
                System.out.println(getGName() + "adsgonj " + kategorieString);
            } catch (SQLException exc) {
                System.out.println(exc.getMessage());
            }
        }
        for (int i = 0; i < sql.kategorien.size(); i++) {
            System.out.println("sdgvhsklgga" + sql.kategorien.get(i) + " " + i);
            centerListe.getChildren().add(new CategoryButton(sql.kategorien.get(i), i).getCategoryButton()); // ersetze ("SimpleLearnerGUI "+i) mit Aufgabenname
        }
        //centerListe.getChildren().add(new CategoryButton("Modul 0", 0).getCategoryButton());
        //centerListe.getChildren().add(new CategoryButton("Modul 1", 1).getCategoryButton());
        //centerListe.getChildren().add(new CategoryButton("Modul 2", 2).getCategoryButton());

        if (isTeacher) {
            centerListe.getChildren().add(new BtnNewElement("Modul").getBtnNeuesElement());
        }
    }

    BorderPane getMainContainer() {
        return mainContainer;

    }

// Verzeichnis-Element

    class QuizButton {

        String btnQuizName;
        Button btnQuiz;
        int taskNumber;

        QuizButton(String input, int nummer) {
            System.out.println("Verzeichnis: Lehrer angemeldet? " + isTeacher());
            btnQuizName = input;
            taskNumber = nummer;
            System.out.println("Aufgabennummer " + taskNumber);
            btnQuiz = new Button(input);
            btnQuiz.getStyleClass().add("VerzeichnisButton");
            System.out.println(btnQuiz.getStyleClass());
            btnQuiz.setPrefWidth(scene.getWidth());
            btnQuiz.setMinWidth(hauptCenter.getWidth()/*-btnLÃ¶schen.getPrefWidth()*/);
            setBtnName(mainStage);
        }

        void setBtnName(Stage tempStage) {
            ContextMenu cMenu = new ContextMenu();
            MenuItem itemDelete = new MenuItem("LÃ¶sche Aufgabenblock");
            MenuItem itemShow = new MenuItem("Zeige SchÃ¼ler");
            cMenu.getItems().addAll(itemDelete, itemShow);

            itemDelete.setOnAction(ae -> {
                try {
                    System.out.println(btnQuizName + loginName.getText() + modulString);
                    sql.deleteBlock(btnQuizName, loginName.getText(), modulString);
                    fillVerzeich();
                } catch (SQLException exc) {
                    System.out.println(exc.getMessage());
                }
            });
            itemShow.setOnAction(ae2 -> {
                Stage pdfStage = new Stage();
                BorderPane bp = new BorderPane();
                VBox vbStudents = new VBox();
                bp.setCenter(vbStudents);
                //VBox vbMehr = new VBox();
                //bp.setLeft(vbMehr);
                try {
                    System.out.println(btnQuizName + loginName.getText());
                    sql.loadAbsolvierteSchueler(btnQuizName, loginName.getText());
                    for (int i = 0; i < sql.absolvierteSchueler.size(); i++) {
                        Button btnStudentName = new Button(sql.absolvierteSchueler.get(i));
                        btnStudentName.setPrefWidth(100);
                        btnStudentName.setOnAction(e -> {
                            FileChooser fc = new FileChooser();
                            fc.getExtensionFilters().add(new FileChooser.ExtensionFilter("PDF (*.pdf)", "*pdf"));
                            File f = fc.showSaveDialog(new Stage());
                            System.out.println(f);
                            if (f != null && !f.getName().contains(".")) {
                                f = new File(f.getAbsolutePath() + ".pdf");
                            }
                            if (f != null) {
                                try {
                                    EvaluationsPdf pdf = new EvaluationsPdf(sql, f);
                                    pdf.createTable(btnQuizName, loginName.getText(), btnStudentName.getText());
                                } catch (IOException | DocumentException | SQLException exc) {
                                    System.out.println(exc.getMessage());
                                }
                            }
                        });
                        vbStudents.getChildren().add(btnStudentName);
                    }
                    Scene scene = new Scene(bp);
                    pdfStage.setScene(scene);
                    pdfStage.show();
                } catch (SQLException exc) {
                    System.out.println(exc.getMessage());
                }
            });
            btnQuiz.setOnMousePressed((MouseEvent e) -> {
                if (isTeacher && e.isSecondaryButtonDown()) {
                    cMenu.show(tempStage, e.getScreenX(), e.getScreenY());
                } else {
                    System.out.println("Aufgabeneinheit wird geÃ¶ffnet:");
                    System.out.println("    gewÃ¤hlte Einheit: " + btnQuizName);
                    System.out.println("------------------------------");
                    start = System.currentTimeMillis();
                    taskNumber = 0;
                    try {
                        sql.loadFragen(btnQuizName);
                    } catch (SQLException exc) {
                        System.out.println(exc.getMessage());
                    }
                    if (sql.fragen.size() > 0) {
                        setBlockPar(btnQuizName);
                        setFragePar(sql.fragen.indexOf(sql.fragen.get(taskNumber)));
                        blockName.setText(btnQuizName);
                        fillAntwortAuswahl(btnQuizName, sql.fragen.get(taskNumber));
                        taskText.setText(sql.fragen.get(taskNumber));
                    } else {
                        setBlockPar(btnQuizName);
                        blockName.setText(btnQuizName);
                        taskText.setText(null);
                        fillAntwortAuswahl(null, null);
                    }
                    //buildAufgabenPane();// Parameter
                    if (isTeacher || (!isTeacher && sql.fragen.size() > 0)) {
                        scene.setRoot(getAufgabenPane());
                        tempStage.setScene(scene);
                        tempStage.setTitle("SimpleLearner - Aufgabe-Nr. " + "***");
                        tempStage.show();
                    }
                }
            });
        }

        BorderPane getQuizButton() {
            BorderPane temp = new BorderPane();
            btnQuiz.setPrefWidth(scene.getWidth());
            temp.setLeft(btnQuiz);
            return temp;
        }
    }

//Kategorie-Element
//
//
//
    class SubjectButton {

        String btnSubjectName;
        Button btnSubject;
        int taskNumber;

        SubjectButton(String input, int nummer) {
            System.out.println("Modul: Lehrer angemeldet? " + isTeacher());
            btnSubjectName = input;
            taskNumber = nummer;
            System.out.println("Kategorie " + taskNumber);
            btnSubject = new Button(input);
            btnSubject.getStyleClass().add("KategorieButton");
            System.out.println(btnSubject.getStyleClass());
            setBtnName(mainStage);

            //btnLoeschen.setPrefWidth(100);
        }

        void setBtnName(Stage tempStage) {
            btnSubject.setOnAction(new EventHandler<ActionEvent>() {
                @Override
                public void handle(ActionEvent e) {
                    System.out.println("Modul wird geÃ¶ffnet:");
                    System.out.println("    gewÃ¤hlte Einheit: " + btnSubjectName);
                    System.out.println("------------------------------");

                    taskNumber = 0;
                    setGName(btnSubjectName);
                    labelDirectory.setText(btnSubjectName);
                    kategorieString = btnSubjectName;
                    hString = "Modul";
                    fillModul(kategorieString);
                    tempStage.setTitle("SimpleLearner - Modul - " + "***");
                    tempStage.show();
                }
            });
        }

        BorderPane getKategorieButton() {
            BorderPane temp = new BorderPane();
            //temp.setLeft(btnQuiz);

            /*if (isTeacher()) {
                btnQuiz.setPrefWidth(scene.getWidth() - 100);
                btnLogout.setLeft(btnQuiz);
                btnLoeschen.setPrefWidth(100);
                btnLogout.setRight(btnLoeschen);
            } else {*/
            btnSubject.setPrefWidth(scene.getWidth());
            temp.setLeft(btnSubject);
            //}
            return temp;
        }
    }

// Kategorie-Element    
//
//
//
    class CategoryButton {

        String btnCategoryName;
        Button btnCategory;
        int aufgabenNummer;

        CategoryButton(String input, int nummer) {
            btnCategoryName = input;
            aufgabenNummer = nummer;
            System.out.println("Modulummer " + aufgabenNummer);
            btnCategory = new Button(input);
            btnCategory.getStyleClass().add("ModulButton");
            System.out.println(btnCategory.getStyleClass());
            btnCategory.setPrefWidth(scene.getWidth());
            btnCategory.setMinWidth(hauptCenter.getWidth()/*-btnLÃ¶schen.getPrefWidth()*/);
            setBtnName(mainStage);

            //btnLÃ¶schen = new Button("Loeschen");
            //btnLÃ¶schen.setStyle("-fx-background-color:rgb(255,50,50)");
            //btnLÃ¶schen.setPrefWidth(100);

            /*
            btnLÃ¶schen.setOnAction(new EventHandler<ActionEvent>() {
                @Override
                public void handle(ActionEvent e){
                    //
                    
                    //
                }
            });
             */
        }

        String getBtnName() {
            return btnCategory.getText();
        }

        void setBtnName(Stage tempStage) {
            btnCategory.setOnAction(new EventHandler<ActionEvent>() {
                @Override
                public void handle(ActionEvent e) {
                    System.out.println("Modul wird geÃ¶ffnet:");
                    System.out.println("    gewÃ¤hlte Einheit: " + btnCategoryName);
                    System.out.println("------------------------------");

                    aufgabenNummer = 0;
                    /*
                    try {
                        sql.loadFragen(btnQuizName);
                    } catch (SQLException exc) {
                        System.out.println(exc.getMessage());
                    }

                    
                    setBlockPar(btnQuizName);
                    setFragePar(sql.fragen.indexOf(sql.fragen.get(taskNumber))); //aufgabenNummer -> modulNummer
                    fillAntwortAuswahl(btnQuizName, sql.fragen.get(taskNumber));
                    aufgabeText.setText(sql.fragen.get(taskNumber));
                     */

                    //buildAufgabenPane();// Parameter
                    setGName(btnCategoryName);
                    labelDirectory.setText(btnCategoryName);
                    modulString = btnCategoryName;
                    hString = "Verzeichnis";
                    fillVerzeich();
                    System.out.println("label : " + getGName());
                    //setScene(getAufgabenPane());
                    //tempStage.setScene(scene);
                    tempStage.setTitle("SimpleLearner - Aufgabe");
                    tempStage.show();
                }
            });
        }

        BorderPane getCategoryButton() {
            BorderPane temp = new BorderPane();
            temp.setLeft(btnCategory);
            /*
            if(isStudent){
                btnLogout.setLeft(btnQuiz);
            }else{
                btnLogout.setLeft(btnQuiz);
                btnLogout.setRight(btnLÃ¶schen);
            }
             */
            return temp;
        }
    }

//AufgabenPane
//
//
//
    String blockPar = null;
    int nummerFragePar = 0;
    long start;
    long end;

    BorderPane topContainerTask = new BorderPane();
    HBox hbBlockName = new HBox();
    Label blockName = new Label("");
    Button btnChangeBlockName = new Button("Blocknamen Ã¤ndern");
    Button btnBlockZurueck = new Button("ZurÃ¼ck");

    BorderPane AufgabenPane = new BorderPane();
    BorderPane tempPane = new BorderPane(); // Ausgabe des Aufgabentextes
    Label taskText = new Label();
    Button btnChangeTaskText = new Button("Fragetext Ã¤ndern");
    VBox vBoxTaskText = new VBox();

    BorderPane AnswerPane = new BorderPane();
    VBox antwortAuswahl = new VBox();
    Button btnAntwortenAuswahl = new Button("Antwortenauswahl Ã¤ndern");
    ToggleGroup AntwortGroup = new ToggleGroup();
    Button btnNeueAntwort = new Button("Neue Aufgabe");
    GridPane navigator = new GridPane();
    //Button btnZurÃ¼ck = new Button("ZurÃ¼ck");
    //Button btnNÃ¤chste = new Button("NÃ¤chste");
    Label auswertungAntwort = new Label();
    Button btnBestaetigen = new Button("BestÃ¤tigen");
    Button btnNaechsteAufgabe = new Button("NÃ¤chste");
    Button btnBeenden = new Button("Beenden");

    
    
    void setVBoxTaskContainer() {
        vBoxTaskText.setMargin(btnChangeTaskText, new Insets(5,5,5,5));
        vBoxTaskText.setMargin(taskText, new Insets(5,5,5,5));
        vBoxTaskText.getChildren().clear();
        vBoxTaskText.getChildren().add(taskText);
        if (isTeacher) {
            vBoxTaskText.getChildren().add(btnChangeTaskText);
        }
    }

    void setBlockPar(String par) {
        blockPar = par;
    }

    void setFragePar(int par) {
        nummerFragePar = par;
    }

    void buildTaskPane() {
        btnBlockZurueck.getStyleClass().add("btn");
        btnChangeBlockName.getStyleClass().add("btn");
        btnChangeTaskText.getStyleClass().add("btn");
        btnAntwortenAuswahl.getStyleClass().add("btn");
        btnBestaetigen.getStyleClass().add("btn");
        
        tempPane.setId("aufgabePane");
        AnswerPane.setId("answerPane");
        topContainerTask.setId("topContainerTask");
        topContainerTask.setPrefHeight(50);

        blockName.setId("blockNameLabel");
        hbBlockName.getChildren().clear();
        hbBlockName.getChildren().add(blockName);
        if (isTeacher) {
            hbBlockName.getChildren().add(btnChangeBlockName);
            btnChangeBlockName.setAlignment(Pos.CENTER_RIGHT);
        }
        topContainerTask.getChildren().clear();
        topContainerTask.setCenter(hbBlockName);
        if (isTeacher) {
            topContainerTask.setLeft(btnBlockZurueck);
        }

        tempPane.setPrefWidth(scene.getWidth() - 250);
        tempPane.setPrefHeight(250);
        tempPane.setId("tempPaneTest");

        taskText.setWrapText(true);
        taskText.setId("taskText");
        taskText.setAlignment(Pos.CENTER);
        taskText.setFont(Font.font(16));
        tempPane.setCenter(vBoxTaskText);
        tempPane.setMaxWidth(400);

        AufgabenPane.setCenter(tempPane);
        AufgabenPane.setRight(AnswerPane);
        AufgabenPane.setTop(topContainerTask);

        AnswerPane.setPrefWidth(250);
        antwortAuswahl.setSpacing(5);

        AnswerPane.setAlignment(antwortAuswahl, Pos.CENTER);
        VBox antworten = new VBox(8);
        antworten.setMargin(btnAntwortenAuswahl, new Insets(10,10,10,10));
        antworten.setMargin(antwortAuswahl, new Insets(15,15,15,15));
        antworten.getChildren().add(antwortAuswahl);
        if (isTeacher) {
            antworten.getChildren().add(btnAntwortenAuswahl);
        }
        AnswerPane.setCenter(antworten);//antwortAuswahl
        //setBtnNeueAufgabe();    //Funktion auskommentiert (Z.338)
        //antwortAuswahl.setBottom(btnNeueAntwort);
        AnswerPane.setAlignment(navigator, Pos.CENTER);
        AnswerPane.setBottom(navigator);
        auswertungAntwort.setFont(Font.font(20));
        navigator.getChildren().clear();
        navigator.add(auswertungAntwort, 0, 0);
        navigator.add(btnBestaetigen, 0, 1);
    }

    void fillAntwortAuswahl(String block, String frage) {
        if (block == null && frage == null) {
            antwortAuswahl.getChildren().clear();
        } else {
            AntwortGroup = new ToggleGroup();
            antwortAuswahl.getChildren().setAll();
            try {
                sql.loadAntworten(block, frage);
            } catch (SQLException exc) {
                System.out.println(exc.getMessage());
            }
            for (int i = 0; i < sql.antwortenTemp.size()/*anzahl der Antworten*/; i++) {
                // hole Aufgabenname der i.ten Aufgabeneinheit
                // Ãœbergebe AufgabenName
                antwortAuswahl.getChildren().add(new btnAntwort(sql.antwortenTemp.get(i)).getBtnAntwort());
            }
        }
    }

    void setBtnLoadAnswerChoice() {

        btnAntwortenAuswahl.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent e) {

                System.out.println("Neue Antwortauswahl");

                Stage tempStage = new Stage();
                tempStage.setTitle("Neuer Aufgabentext");

                ToggleGroup tempToggleGroup = new ToggleGroup();
                VBox vBox = new VBox();
                HBox hBox = new HBox();
                Button btnAbbr = new Button("Abbrechen");
                Button btnBestaet = new Button("BestÃ¤tigen");
                // vBox fÃ¼llen
                //HBox h1 = new HBox();
                //    h1.getChildren().add(new Button());
                //    h1.getChildren().add(new TextField());
                //    vBox.getChildren().add(h1);
                for (int i = 0; i < antwortAuswahl.getChildren().size(); i++) {
                    RadioButton rb = (RadioButton) antwortAuswahl.getChildren().get(i);
                    String text = rb.getText();
                    RadioButton neuRb = new RadioButton();
                    neuRb.setToggleGroup(tempToggleGroup);
                    TextField neuTf = new TextField(text);
                    HBox hb = new HBox();
                    hb.getChildren().addAll(neuRb, neuTf);
                    vBox.getChildren().add(hb);
                }
                VBox vBox2 = new VBox();
                Button btnNeueAnwort = new Button("Neue Antwort");
                vBox2.getChildren().add(vBox);
                vBox2.getChildren().add(btnNeueAntwort);

                hBox.getChildren().add(btnAbbr);
                hBox.getChildren().add(btnBestaet);

                BorderPane tempPane = new BorderPane();
                tempPane.setCenter(vBox2);
                tempPane.setBottom(hBox);

                Scene tempScene = new Scene(tempPane, 300, 300);

                tempStage.setScene(tempScene);
                tempStage.show();

                // btnNeueAntwort definieren
                btnNeueAntwort.setOnAction(new EventHandler<ActionEvent>() {
                    @Override
                    public void handle(ActionEvent e) {
                        System.out.println("Neue Antwort wird erstellt");

                        RadioButton radioBtn = new RadioButton();
                        radioBtn.setToggleGroup(tempToggleGroup);

                        HBox h = new HBox();
                        h.getChildren().add(radioBtn);
                        h.getChildren().add(new TextField());
                        vBox.getChildren().add(h);

                    }
                });

                // btnAbbr definieren
                btnAbbr.setOnAction(new EventHandler<ActionEvent>() {
                    @Override
                    public void handle(ActionEvent e) {

                        System.out.println("Eingabe abgebrochen");
                        tempStage.close();
                    }
                });

                btnBestaet.setOnAction(new EventHandler<ActionEvent>() {
                    @Override
                    public void handle(ActionEvent e) {

                        System.out.println("Eingabe wird gespeichert");

                        try {
                            sql.updateAnswers(blockPar, taskText.getText(), loginName.getText(), vBox);
                            setBtnLoadAnswerChoice();
                            fillAntwortAuswahl(blockPar, taskText.getText());
                            //hier die Antworten direkt neu Laden in der VBox
                        } catch (SQLException exc) {
                            System.out.println(exc.getMessage());
                        }
                        /*
                           Anweisungen hier
                         */
                        // Auslesen aller TextFelder
                        for (int i = 0; i < vBox.getChildren().size(); i++) {
                            HBox hb = (HBox) vBox.getChildren().get(i);
                            for (int j = 0; j < hb.getChildren().size(); j++) {
                                if (hb.getChildren().get(j) instanceof TextField) {
                                    TextField tf = (TextField) hb.getChildren().get(j);
                                    String ausgabeString = tf.getText();
                                    System.out.println(ausgabeString + "ausgabeString");
                                }
                            }
                        }
                        /*
                        // vergleiche toggles mit gewÃ¤hlten toggle zur feststellung der wievielte er ist.            
                        int nrSelectedToggle = -1;

                        for (int i = 0; i < tempToggleGroup.getToggles().size(); i++) {
                            if (tempToggleGroup.getSelectedToggle().equals(tempToggleGroup.getToggles().get(i))) {
                                nrSelectedToggle = i;
                                break;
                            }
                        }
                        System.out.println(nrSelectedToggle);
                         */
                        //Antworten neuladen und Fenster schliessen
                        //btnLabel und taskNumber sind unbekannt 
                        //fillAntwortAuswahl(*btnQuizName*, sql.fragen.get(*taskNumber*));
                        tempStage.close();
                    }
                });

            }
        });
    }

    void setBtnBlockName() {
        btnChangeBlockName.setOnAction(e -> {
            Stage tempStage = new Stage();
            tempStage.setTitle("Neuer Quizname");

            BorderPane borderPane = new BorderPane();
            TextField tf = new TextField();
            tf.setPromptText("Hier Blocknamen eintragen");
            Button btnAbbrechen = new Button("Abbrechen");
            Button btnBestaetigen = new Button("Bestaetigen");

            btnAbbrechen.setOnAction(e2 -> {
                tempStage.close();
            });
            btnBestaetigen.setOnAction(e3 -> {
                try {
                    sql.updateQuiz(blockPar, loginName.getText(), tf.getText());
                    blockPar = tf.getText();
                    blockName.setText(tf.getText());
                } catch (SQLException exc) {
                    System.out.println(exc.getMessage());
                }
                tempStage.close();
            });
            borderPane.setTop(tf);
            borderPane.setLeft(btnAbbrechen);
            borderPane.setRight(btnBestaetigen);

            Scene tempScene = new Scene(borderPane, 300, 300);

            tempStage.setScene(tempScene);
            tempStage.show();
        });
    }

    void setBtnBack(Stage tempStage) {//Ã¶ffnet nÃ¤chste Aufgabe
        btnBlockZurueck.setOnAction((ActionEvent e) -> {
            System.out.println("aufgabe beendet");
            //sql.endAufgabe();
            fillVerzeich();
            scene.setRoot(getMainContainer());
            tempStage.setScene(scene);
            tempStage.setTitle("SimpleLearner - Kategorie");
            tempStage.show();
        });
    }

    void setBtnNewQuestionText() {

        btnChangeTaskText.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent e) {

                System.out.println("Neuer Aufgabentext");

                Stage tempStage = new Stage();
                tempStage.setTitle("Neuer Aufgabentext");

                HBox hBox = new HBox();
                Button btnAbbr = new Button("Abbrechen");
                Button btnBestaet = new Button("BestÃ¤tigen");
                hBox.getChildren().add(btnAbbr);
                hBox.getChildren().add(btnBestaet);

                TextArea tempTextArea = new TextArea("AufgabenText hier eingeben");
                tempTextArea.setMinWidth(300);
                tempTextArea.setMinHeight(250);
                tempTextArea.setWrapText(true);

                BorderPane tempPane = new BorderPane();
                tempPane.setCenter(tempTextArea);
                tempPane.setBottom(hBox);

                Scene tempScene = new Scene(tempPane, 300, 300);

                tempStage.setScene(tempScene);
                tempStage.show();

                // btnAbbr definieren
                btnAbbr.setOnAction(new EventHandler<ActionEvent>() {
                    @Override
                    public void handle(ActionEvent e) {

                        System.out.println("Eingabe abgebrochen");
                        tempStage.close();
                    }
                });

                btnBestaet.setOnAction(new EventHandler<ActionEvent>() {
                    @Override
                    public void handle(ActionEvent e) {

                        System.out.println("Eingabe wird gespeichert");

                        /*
            Anweisungen hier
                - TextArea auslesen
                - String Ã¼bergeben
                         */
                        System.out.println(">> String-Ausgabe >> " + tempTextArea.getText() + blockPar + " bla" + taskText.getText());
                        try {
                            sql.updateTask(blockPar, taskText.getText(), tempTextArea.getText());
                            taskText.setText(tempTextArea.getText());
                        } catch (SQLException e2) {
                            System.out.println(e2.getMessage());
                        }
                        tempStage.close();
                    }
                });

            }
        });
    }

    void setBtnConfirmAnswer() {//wertet den bewÃ¤hlte Button aus und gibt diese aus
        btnBestaetigen.setPrefWidth(75);
        btnBestaetigen.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent e) {
                String antwort = null;
                if (AntwortGroup.getSelectedToggle() != null) {
                    antwort = AntwortGroup.getSelectedToggle().getUserData().toString();
                }
                System.out.println("Auswahl wurde bestÃ¤tigt");
                System.out.println("    GewÃ¤hlte Antwort: " + antwort);

                //checkAntwort(antwort);
                if (!isTeacher) {
                    if (antwort != null) {
                        try {
                            sql.startBlock(blockPar, loginName.getText());
                            end = System.currentTimeMillis() - start;
                            System.out.println(end); //Zeitmessen funktioniert und muss nur noch Ã¼bergeben werden an checkAntwort
                            if (sql.checkAntwort(blockPar, loginName.getText(), sql.fragen.get(nummerFragePar), antwort) == true) {
                                System.out.println("richtig");
                                auswertungAntwort.setText("richtig");
                            } else {
                                System.out.println("falsch");
                                auswertungAntwort.setText("falsch");
                            }
                            System.out.println("------------------------------");
                        } catch (SQLException exc) {
                            System.out.println(exc.getMessage());
                        }
                    }
                }
                //ersetze "BestÃ¤tigen"-Button mit "NÃ¤chste"-Button
                if (!isTeacher && antwort != null) {
                    if (nummerFragePar < sql.fragen.size() - 1) {
                        navigator.add(btnNaechsteAufgabe, 0, 1);
                    } else {
                        navigator.add(btnBeenden, 0, 1);
                    }
                } else {
                    navigator.add(btnNaechsteAufgabe, 0, 1);
                }
                /*else {
                    Stage stage = new Stage();
                    Label labelDirectory = new Label("Sie haben das Quiz vollstÃ¤ndig bearbeitet.");
                    Button btnStudentName = new Button("ZurÃ¼ck zu den Aufgaben");
                    VBox vbStudents = new VBox();
                    vbStudents.getChildren().addAll(labelDirectory, btnStudentName);
                    Scene scene = new Scene(vbStudents);
                    stage.setScene(scene);
                    stage.show();
                }*/

            }
        });
    }

    void setBtnNextTask(Stage tempStage) {//Ã¶ffnet nÃ¤chste Aufgabe
        btnNaechsteAufgabe.setPrefWidth(75);
        btnNaechsteAufgabe.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent e) {
                System.out.println("NÃ¤chste Aufgabe wird geÃ¶ffnet");
                System.out.println("    Neue Aufgabe: ");
                System.out.println("------------------------------");

                start = System.currentTimeMillis();
                // exception durch erneutes EinfÃ¼gen von btnBestÃ¤tigen
                // -> btnBestÃ¤tigen lÃ¶schen und erneut einfÃ¼gen
                navigator.getChildren().remove(1, 3); // btnBestÃ¤tigen und btnNÃ¤chsteAufgabe lÃ¶schen
                navigator.add(btnBestaetigen, 0, 1); // btnBestÃ¤tigen ein
                System.out.println("    >BestÃ¤tigen-Button eingefÃ¼gt");
                if (isTeacher && (nummerFragePar + 1 >= sql.fragen.size())) {
                    taskText.setText(null);
                    fillAntwortAuswahl(null, null);
                } else {
                    taskText.setText(sql.fragen.get(nummerFragePar + 1));
                    nummerFragePar++;
                    System.out.println("    >AufgabenText geÃ¤ndert");
                    fillAntwortAuswahl(blockPar, sql.fragen.get(nummerFragePar));
                }
                auswertungAntwort.setText("");
                System.out.println("    >AntwortAuswahl erneuert");
                System.out.println("------------------------------");

                tempStage.show();
            }
        });
    }

    void setBtnConfirmQuiz(Stage tempStage) {//Ã¶ffnet nÃ¤chste Aufgabe
        btnBeenden.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent e) {
                System.out.println("aufgabe beendet");
                //sql.endAufgabe();
                auswertungAntwort.setText("");
                navigator.getChildren().remove(1, 3);
                navigator.add(btnBestaetigen, 0, 1);
                fillVerzeich();
                scene.setRoot(getMainContainer());
                tempStage.setScene(scene);
                tempStage.setTitle("SimpleLearner - Kategorie");
                tempStage.show();
            }
        });
    }

    BorderPane getAufgabenPane() {
        return AufgabenPane;

    }

    class btnAntwort {

        RadioButton btn;
        String btnLabel;

        btnAntwort(String input) {
            btnLabel = input;
            btn = new RadioButton(btnLabel);
            btn.setUserData(btnLabel);
            btn.setToggleGroup(AntwortGroup);
        }

        void setBtnAntwort() {
            btn.setOnAction(new EventHandler<ActionEvent>() {
                @Override
                public void handle(ActionEvent e) {
                    System.out.println(btnLabel);
                }
            });
        }

        RadioButton getBtnAntwort() {
            return btn;
        }
    }
// Scene    
// getMainContainer();
// getAufgabenPane();
//Scene scene = new Scene(changeMeldungPane(getLoginPane()), 600, 600);
    Scene scene = new Scene(getLoginPane(), 600, 600);

    void loadStyleSheets() {
        scene.getStylesheets().add("SEProject/SimpleLearnerGUI.css");
    }

    Stage mainStage = new Stage();

    void setPrimaryStage() {
        mainStage.setScene(scene);
    }
}