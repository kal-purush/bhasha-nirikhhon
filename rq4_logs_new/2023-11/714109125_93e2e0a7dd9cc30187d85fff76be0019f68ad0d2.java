
package inventarios;

import java.net.URL;
import java.util.ResourceBundle;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.geometry.Side;
import javafx.scene.Group;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.MenuButton;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.layout.StackPane;
import javafx.scene.text.Text;

/**
 * FXML Controller class
 *
 * @author diana
 */
public class FXMLMenuController implements Initializable {

    @FXML
    private Text txtTitle;
    @FXML
    private MenuButton btnAdmin;
    @FXML
    private TabPane tabpane;
    @FXML
    private Tab inventario;
    @FXML
    private Tab consultas;
    @FXML
    private Tab centros;
    @FXML
    private Tab nose;
    @FXML
    private Button btnConsultarPerifericos;
    @FXML
    private Button btnConsultarCC;
    @FXML
    private Button btnConsultarUsuario;
    @FXML
    private Button btnConsultarSoftware;
    @FXML
    private Button btnConsultarEquipo;
    @FXML
    private Tab nose1;

    @Override
    public void initialize(URL url, ResourceBundle rb) {
        
    }


    @FXML
    private void cerrarSesion(ActionEvent event) {
    }

    @FXML
    private void clicBtnConsultarPerifericos(ActionEvent event) {
    }

    @FXML
    private void clicBtnConsultarCC(ActionEvent event) {
    }

    @FXML
    private void clicBtnConsultarUsuarios(ActionEvent event) {
    }

    @FXML
    private void clicBtnConsultarSoftware(ActionEvent event) {
    }

    @FXML
    private void clicBtnConsultarEquipo(ActionEvent event) {
    }
    
}