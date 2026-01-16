/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/javafx/FXMLController.java to edit this template
 */
package inventarios.centroscomputo;

import inventarios.dao.CentroComputoDAO;
import inventarios.pojo.CentroComputo;
import inventarios.util.Utilidades;
import java.net.URL;
import java.sql.SQLException;
import java.util.List;
import java.util.ResourceBundle;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.stage.Stage;

/**
 * FXML Controller class
 *
 * @author LENOVO
 */
public class FXMLConsultaCCController implements Initializable {

    @FXML
    private TableView<CentroComputo> tvCC;
    @FXML
    private TableColumn<?, ?> clClave;
    @FXML
    private TableColumn<?, ?> clNumero;
    @FXML
    private Button btnModificar;
    @FXML
    private Button btnEliminar;
    
    private ObservableList<CentroComputo> listaCCs = FXCollections.observableArrayList();

    /**
     * Initializes the controller class.
     */
    @Override
    public void initialize(URL url, ResourceBundle rb) {
        clClave.setCellValueFactory(new PropertyValueFactory<>("clave"));
        clNumero.setCellValueFactory(new PropertyValueFactory<>("numero"));
        
        tvCC.setItems(listaCCs);
        llenarTablaCCs();
    }    

    @FXML
    private void clicBtnVolver(ActionEvent event) {
        Stage stage = (Stage) btnEliminar.getScene().getWindow();
        stage.close();
    }

    @FXML
    private void clicBtnModificarCC(ActionEvent event) {
    }

    @FXML
    private void clicBtnEliminarCC(ActionEvent event) {
    }
    
     private void llenarTablaCCs(){
        
         CentroComputoDAO ccDao = new CentroComputoDAO();
            try {
                List<CentroComputo> resultados = ccDao.consultarCCs();
                listaCCs.clear();
                if (resultados != null) {
                listaCCs.addAll(resultados);
                }else{
                    listaCCs.addAll(resultados);
                }
            } catch (SQLException ex) {
                Utilidades.mostrarAlertaSimple("Error", "Error en la conexión con la base de datos. Intente de nuevo más tarde.", Alert.AlertType.ERROR);
            }
    }
    
}