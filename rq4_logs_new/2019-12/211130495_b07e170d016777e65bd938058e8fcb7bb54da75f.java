package adminPage;

import application.FermiConnector;
import application.FermiEntry;
import editUsersPage.EditUsersView;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.print.*;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.Pane;
import javafx.scene.transform.Scale;
import javafx.stage.Stage;
import login.LoginController;

import java.awt.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class AdminController {
    private FermiConnector base = new FermiConnector();

    @FXML
    private TableView<FermiEntry> dataTable;

    @FXML
    TableColumn<FermiEntry, String> firstNameCol;

    @FXML
    TableColumn<FermiEntry, String> lastNameCol;

    @FXML
    TableColumn<FermiEntry, String> phoneCol;

    @FXML
    TableColumn<FermiEntry, Double> overCol;

    @FXML
    TableColumn<FermiEntry, Integer> senCol;

    @FXML
    TableColumn<FermiEntry, Boolean> bisonCol;

    @FXML
    private Button printButton;
    
    @FXML
    private Button logoutButton;

    public AdminController() {

    }

    @FXML
    private void initialize() {
        firstNameCol.setCellValueFactory(new PropertyValueFactory("firstName"));
        lastNameCol.setCellValueFactory(new PropertyValueFactory("lastName"));
        phoneCol.setCellValueFactory(new PropertyValueFactory("phone"));
        overCol.setCellValueFactory(new PropertyValueFactory("overtime"));
        senCol.setCellValueFactory(new PropertyValueFactory("seniority"));
        bisonCol.setCellValueFactory(new PropertyValueFactory("inBison"));

        dataTable.setItems(getData());
    }

    private ObservableList<FermiEntry> getData() {
        ObservableList<FermiEntry> data = FXCollections.observableArrayList();

        try {
            Statement stmt = base.getConn().createStatement();
            ResultSet result = stmt.executeQuery("SELECT * FROM hours_offered");
            while (result.next()) {
                data.add(new FermiEntry(result.getString(1), result.getString(2), result.getString(3), result.getDouble(4),
                        result.getInt(5), result.getBoolean(6)));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return data;
    }

    @FXML
    private void printPage() {
        Node root = dataTable;
        PrinterJob job = PrinterJob.createPrinterJob();

        if (job != null && job.showPrintDialog(printButton.getScene().getWindow())) {
            Printer printer = job.getPrinter();
            PageLayout pageLayout = printer.createPageLayout(Paper.A4, PageOrientation.PORTRAIT, Printer.MarginType.HARDWARE_MINIMUM);

            double width = 600;
            double height = 800;

            PrintResolution resolution = job.getJobSettings().getPrintResolution();

            width /= resolution.getFeedResolution();

            height /= resolution.getCrossFeedResolution();

            double scaleX = pageLayout.getPrintableWidth() / width / 600;
            double scaleY = pageLayout.getPrintableHeight() / height / 600;

            Scale scale = new Scale(scaleX, scaleY);

            root.getTransforms().add(scale);

            boolean success = job.printPage(pageLayout, root);
            if (success) {
                job.endJob();
            }

            root.getTransforms().remove(scale);
        }
    }

    @FXML
    private void editUsers() throws Exception {
        Stage stage = (Stage) printButton.getScene().getWindow();
        EditUsersView view = new EditUsersView();
        view.showView(stage);
    }
    
    @FXML
    private void logOut() throws Exception{
    	Stage stage = (Stage) logoutButton.getScene().getWindow();

        LoginController view = new LoginController();
        view.showView(stage);
    }
}