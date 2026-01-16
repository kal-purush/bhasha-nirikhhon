/**
 * Sample Skeleton for 'catalog.fxml' Controller Class
 */

package il.cshaifasweng.OCSFMediatorExample.client;

import java.awt.event.ActionEvent;
import java.io.IOException;

import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.ListView;
import javafx.scene.text.Text;

public class CatalogController {

    @FXML // fx:id="PageTitle"
    private Text PageTitle; // Value injected by FXMLLoader

    @FXML // fx:id="List"
    private ListView<?> List; // Value injected by FXMLLoader

    @FXML // fx:id="returnHomeBTN"
    private Button returnHomeBTN; // Value injected by FXMLLoader

    @FXML
    private void switchToPrimary() throws IOException {
        App.setRoot("primary");
    }

}