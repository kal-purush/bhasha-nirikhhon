
package inventarios.equipocomputo;

import inventarios.dao.CentroComputoDAO;
import inventarios.dao.EquipoDAO;
import inventarios.pojo.CentroComputo;
import inventarios.pojo.Equipo;
import inventarios.pojo.ResultadoOperacion;
import inventarios.util.Utilidades;
import java.io.IOException;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ResourceBundle;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.ComboBox;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.stage.Modality;
import javafx.stage.Stage;

public class FXMLConsultaEquiposComputoController implements Initializable {

    @FXML
    private ComboBox<String> cbCC;
    @FXML
    private Button btnVolver;
    @FXML
    private Button btnConsultar;
    @FXML
    private TableView<Equipo> tvEquiposComputo;
    @FXML
    private TableColumn clIdentificador;
    @FXML
    private TableColumn clProcesador;
    @FXML
    private TableColumn clMemoriaRAM;
    @FXML
    private TableColumn clCantidadRAM;
    @FXML
    private TableColumn clAlmacenamiento;
    @FXML
    private TableColumn clEspacio;
    @FXML
    private TableColumn clUbicacion;
    @FXML
    private TableColumn clSistemaOperativo;
    @FXML
    private Button btnModificar;
    @FXML
    private Button btnEliminar;
    
    private ObservableList<String> listaCC;
    
    private ObservableList<Equipo> listaEquipos = FXCollections.observableArrayList();
    
    

    /**
     * Initializes the controller class.
     */
    @Override
    public void initialize(URL url, ResourceBundle rb) {
        llenarCombos();
        
        clIdentificador.setCellValueFactory(new PropertyValueFactory<>("identificador"));
        clProcesador.setCellValueFactory(new PropertyValueFactory<>("procesador"));
        clMemoriaRAM.setCellValueFactory(new PropertyValueFactory<>("memoriaRAM"));
        clCantidadRAM.setCellValueFactory(new PropertyValueFactory<>("memoriaRAMcantidad"));
        clAlmacenamiento.setCellValueFactory(new PropertyValueFactory<>("tipoalmacenamiento"));
        clEspacio.setCellValueFactory(new PropertyValueFactory<>("espacioalmacenamiento"));
        clUbicacion.setCellValueFactory(new PropertyValueFactory<>("ubicacionfisica"));
        clSistemaOperativo.setCellValueFactory(new PropertyValueFactory<>("sistemaoperativo"));
        
        tvEquiposComputo.setItems(listaEquipos);
    }

    private void llenarCombos(){
        ArrayList<String> ccs = new ArrayList<String>();
        ArrayList<CentroComputo> ccConsulta;
        try {
            ccConsulta = CentroComputoDAO.consultarCCs();

            for (CentroComputo cc : ccConsulta) {
                String clavee = cc.getClave();
                ccs.add(clavee);
            }
        } catch (SQLException ex) {
            Utilidades.mostrarAlertaSimple("Error", "Error en la conexión con la base de datos. Intente de nuevo más tarde.", Alert.AlertType.ERROR);
        }
        listaCC = FXCollections.observableArrayList(ccs);
        cbCC.setItems(listaCC);
    }

    @FXML
    private void consultarEquipos(ActionEvent event) {
        
        String cc = cbCC.getValue();
        if(cc == null || cc.isEmpty()){
            Utilidades.mostrarAlertaSimple("Campo vacío", "Debe seleccionar un centro de cómputo", Alert.AlertType.ERROR);
        }else{
            EquipoDAO eDao = new EquipoDAO();
            
            try {
                List<Equipo> resultados = eDao.consultarEquiposComputo(1);
                listaEquipos.clear();
                if(resultados != null){
                    listaEquipos.addAll(resultados);
                }else{
                    listaEquipos.addAll(resultados);
                }
            } catch (SQLException e) {
                Utilidades.mostrarAlertaSimple("ERROR", "Error en la conexión con la base de datos. Intentelo más tarde.", Alert.AlertType.ERROR);
            }
        }
        
    }

    @FXML
    private void modificarEquipo(ActionEvent event) {
        Equipo equipoSeleccionado = tvEquiposComputo.getSelectionModel().getSelectedItem();
        if (equipoSeleccionado == null) {
            Utilidades.mostrarAlertaSimple("Falta selección", "Debe seleccionar un registro de la tabla", Alert.AlertType.WARNING);
        } else {
            try {
                FXMLLoader loader = new FXMLLoader(getClass().getResource("FXMLModificaEquipoComputo.fxml"));
                Parent vista = loader.load();
                FXMLModificaEquipoComputoController edicionController = loader.getController();
                edicionController.setEquipo(equipoSeleccionado);

                Scene escenaAdmin = new Scene(vista);
                Stage escenarioNuevo = new Stage();
                escenarioNuevo.setTitle("Edición de Equipo de computo");
                escenarioNuevo.setScene(escenaAdmin);
                escenarioNuevo.initModality(Modality.APPLICATION_MODAL);
                escenarioNuevo.showAndWait();
            } catch (IOException e) {
                Utilidades.mostrarAlertaSimple("Error", "Error al cargar la página.", Alert.AlertType.ERROR);
            }
        }
    }

    @FXML
    private void eliminarEquipo(ActionEvent event) {
        Equipo equipoSeleccionado = tvEquiposComputo.getSelectionModel().getSelectedItem();
        if(equipoSeleccionado == null){
            Utilidades.mostrarAlertaSimple("Falta selección", "Debe seleccionar un registro de la tabla.", Alert.AlertType.WARNING);
        }else{
            Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
            alert.setTitle("Confirmación de eliminación");
            alert.setHeaderText("¿Desea eliminar el equipo de cómputo seleccionado?");
            alert.setContentText("Esta acción no se puede deshacer.");

            Optional<ButtonType> result = alert.showAndWait();
            if(result.get() == ButtonType.OK){
                EquipoDAO eDao = new EquipoDAO();
                ResultadoOperacion resultado;
                try {
                    resultado = eDao.eliminarEquipo(equipoSeleccionado.getIdEquipoComputo());
                    if(resultado.isError()){
                        Utilidades.mostrarAlertaSimple("Error", "Error en la eliminación.", Alert.AlertType.ERROR);
                    }else{
                       Utilidades.mostrarAlertaSimple("Eliminación exitosa", "Equipo de cómputo eliminado con éxito.", Alert.AlertType.INFORMATION); 
                       listaEquipos.remove(equipoSeleccionado);
                       tvEquiposComputo.refresh();
                    }
                } catch (SQLException e) {
                    Utilidades.mostrarAlertaSimple("Error", "No se pudo eliminar el registro de periférico.", Alert.AlertType.ERROR);
                }
            }else{
                alert.close();
            }
        }
    }

    @FXML
    private void volver(ActionEvent event) {
        Stage stage = (Stage) cbCC.getScene().getWindow();
        stage.close();
    }
    
}