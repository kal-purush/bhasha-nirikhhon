
package inventarios.centroscomputo;

import inventarios.dao.CentroComputoDAO;
import inventarios.pojo.CentroComputo;
import inventarios.pojo.ResultadoOperacion;
import inventarios.util.Utilidades;
import java.net.URL;
import java.sql.SQLException;
import java.util.Optional;
import java.util.ResourceBundle;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Alert;
import javafx.scene.control.ButtonType;
import javafx.scene.control.TextField;
import javafx.stage.Stage;

/**
 * FXML Controller class
 *
 * @author diana
 */
public class FXMLModificaCCController implements Initializable {
    CentroComputo oldCC = new CentroComputo();
    @FXML
    private TextField tfClave;
    @FXML
    private TextField tfNumero;

    @Override
    public void initialize(URL url, ResourceBundle rb) {
        
    }    

    @FXML
    private void clicBtnCancelar(ActionEvent event) {
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Confirmación");
        alert.setHeaderText("¿Desea cancelar la operación?");

        Optional<ButtonType> result = alert.showAndWait();
        if (result.get() == ButtonType.OK) {
            Stage escenarioRegistro = (Stage) tfClave.getScene().getWindow();
            escenarioRegistro.close(); 
        }else{
            alert.close();
        }
    }

    @FXML
    private void clicBtnModificar(ActionEvent event) {
        String clave = tfClave.getText();
        String numero = tfNumero.getText();
        
        if(clave.isEmpty() || validarCaracteres(clave) == false){
            
            Utilidades.mostrarAlertaSimple("Error", "No puede haber campos vacíos o caracteres inválidos", Alert.AlertType.ERROR);
            
        }else if(numero.isEmpty() || validarCaracteresEnteros(numero) == false){
            
            Utilidades.mostrarAlertaSimple("Error", "No puede haber campos vacíos o caracteres inválidos", Alert.AlertType.ERROR);
           
        }else{
            
             try{
                CentroComputo ccBuscar = new CentroComputoDAO().buscarCC(clave);
                 
                if(ccBuscar.getNumero() < 0){
                     
                    int numeroEntero = Integer.parseInt(numero);
                    CentroComputo cc = new CentroComputo(clave, numeroEntero);

                    CentroComputoDAO cDao = new CentroComputoDAO();
                    ResultadoOperacion resultado = cDao.modificarCC(oldCC.getIdCC(), cc);
                    if(resultado.isError()){
                        Utilidades.mostrarAlertaSimple("Error", resultado.getMensaje(), Alert.AlertType.ERROR);
                    
                    }else{
                        Utilidades.mostrarAlertaSimple("Exito", "Centro de computo registrado con exito", Alert.AlertType.INFORMATION);
                        Stage escenarioRegistro = (Stage) tfClave.getScene().getWindow();
                        escenarioRegistro.close(); 
                    }
                }else{
                    Utilidades.mostrarAlertaSimple("Error", "La clave que intenta registrar, ya se encuentra en la base de datos", Alert.AlertType.ERROR);
                }
                    
                }catch(SQLException e){
                    Utilidades.mostrarAlertaSimple("Error", "Error en la conexión con la base de datos. Intente de nuevo más tarde", Alert.AlertType.ERROR);
                }
        }
    }
    
    private boolean validarCaracteres(String cadena){
        return cadena.matches("-?([a-zA-Z_0-9]*)?");
    }
    
    private boolean validarCaracteresEnteros(String cadena){
        return cadena.matches("\\d+");
    }

    void set(CentroComputo ccSeleccionado) {
        this.oldCC = ccSeleccionado;
        
        String oldclave = oldCC.getClave();
        int oldNumero = oldCC.getNumero();
        String cadena = String.valueOf(oldNumero);
        int oldId = oldCC.getIdCC();
        tfClave.setText(oldclave);
        tfNumero.setText(cadena);
    }
}