package reportPage;

import application.FermiEntry;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.control.TableView;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;

public class ReportsView {
   @FXML
   private TableView<FermiEntry> dataTable;

    public ReportsView() {
    }

    public void showView(Stage window) throws Exception {
        Pane root = (Pane) FXMLLoader.load(getClass().getResource("reportPage.fxml"));
        Scene scene = new Scene(root, 620, 640);
        window.setTitle("Reports");
        window.setScene(scene);
        window.setResizable(false);
        window.show();
    }
}