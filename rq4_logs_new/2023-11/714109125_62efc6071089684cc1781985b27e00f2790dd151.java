
package inventarios.bitacora;

import inventarios.dao.BitacoraDAO;
import inventarios.dao.EquipoDAO;
import inventarios.pojo.Bitacora;
import inventarios.pojo.Equipo;
import inventarios.pojo.EquipoRespuesta;
import inventarios.util.Constantes;
import inventarios.util.Utilidades;
import java.net.URL;
import java.util.ArrayList;
import java.util.ResourceBundle;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Alert;
import javafx.scene.control.ComboBox;
import javafx.scene.control.DatePicker;
import javafx.scene.control.TextArea;
import javafx.stage.Stage;

/**
 * FXML Controller class
 *
 * @author miran
 */
public class FXMLRegistraBitacoraController implements Initializable {

    @FXML
    private TextArea taDescripcion;
    @FXML
    private DatePicker dpFecha;
    @FXML
    private ComboBox<Equipo> cbEquipoComputo;
    private ObservableList<String> listaEquipos;
    private ObservableList<Equipo> equipos;


    @Override
    public void initialize(URL url, ResourceBundle rb) {
        llenarCombo();
    }

    @FXML
    public void registrar(ActionEvent actionEvent) {
        Bitacora bitacora = new Bitacora();
        bitacora.setFecha(dpFecha.getValue());
        bitacora.setDescripcion(taDescripcion.getText());
        boolean respuesta = BitacoraDAO.registrarBitacora(bitacora);
        if (respuesta) {
            Utilidades.mostrarAlertaSimple("Registro exitoso", "Bitácora registrada con éxito.", Alert.AlertType.INFORMATION);
            Stage stage = (Stage) dpFecha.getScene().getWindow();
            stage.close();
        } else {
            Utilidades.mostrarAlertaSimple("Error", "No se pudo registrar la bitácora.", Alert.AlertType.ERROR);
        }
        cleanFields();
    }

    @FXML
    public void regresar(ActionEvent actionEvent) {
        Stage stage = (Stage) dpFecha.getScene().getWindow();
        stage.close();
    }

    private void cleanFields() {
        dpFecha.setValue(null);
        taDescripcion.setText("");
    }
    
    public void llenarCombo(){
        equipos = FXCollections.observableArrayList();
        EquipoRespuesta respuestaBD = EquipoDAO.obtenerInformacionEquipo();
        switch(respuestaBD.getCodigoRespuesta()){
            case Constantes.ERROR_CONEXION:
                Utilidades.mostrarAlertaSimple("Sin conexion",
                        "Por el momento no hay conexión con la base de datos, por favor reintente más tarde",
                        Alert.AlertType.ERROR);
                break;
            case Constantes.ERROR_CONSULTA:
                Utilidades.mostrarAlertaSimple("Error de conexión con la base de datos",
                        "Por favor inténtelo más tarde",
                        Alert.AlertType.WARNING);
                break;
            case Constantes.OPERACION_EXITOSA:
                equipos.addAll(respuestaBD.getEquipoLista());
                cbEquipoComputo.setItems(equipos);
                break;
        }
    }
}