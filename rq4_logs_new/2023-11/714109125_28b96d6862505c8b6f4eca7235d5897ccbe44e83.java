/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/javafx/FXMLController.java to edit this template
 */
package inventarios.centroscomputo;

import java.net.URL;
import java.sql.SQLException;
import java.util.ResourceBundle;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Alert;
import javafx.scene.control.TextField;
import javafx.stage.Stage;
import inventarios.dao.CentroComputoDAO;
import inventarios.pojo.CentroComputo;
import inventarios.util.Utilidades;

/**
 * FXML Controller class
 *
 * @author LENOVO
 */
public class FXMLRegistraCCController implements Initializable {

    @FXML
    private TextField tfClave;
    @FXML
    private TextField tfNumero;

    /**
     * Initializes the controller class.
     */
    @Override
    public void initialize(URL url, ResourceBundle rb) {
        // TODO
    }    

    @FXML
    private void clicBtnRegistrar(ActionEvent event) {
        String clave = tfClave.getText();
        String numero = tfNumero.getText();
        
        if(clave.isEmpty() || validarCaracteres(clave) == false){
            
            Utilidades.mostrarAlertaSimple("Error", "No puede haber campos vacíos o caracteres inválidos", Alert.AlertType.ERROR);
            
        }else if(numero.isEmpty() || validarCaracteresEnteros(numero) == false){
            
            Utilidades.mostrarAlertaSimple("Error", "No puede haber campos vacíos o caracteres inválidos", Alert.AlertType.ERROR);
           
        }else{
            
             try{
                    int numeroEntero = Integer.parseInt(numero);
                    CentroComputo cc = new CentroComputo(clave, numeroEntero);
                    
                    if(CentroComputoDAO.registrarCentroComputo(cc) == false){
                        Utilidades.mostrarAlertaSimple("Error", "No se ha podido registrar el Centro de computo", Alert.AlertType.ERROR);
                    
                    }else{
                        Utilidades.mostrarAlertaSimple("Exito", "Centro de computo registrado con exito", Alert.AlertType.INFORMATION);
                        cerrarVentana();
                    }
                    
                }catch(SQLException e){
                    Utilidades.mostrarAlertaSimple("Error", "Error en la conexión con la base de datos. Intente de nuevo más tarde", Alert.AlertType.NONE);
                }
        }
    }

    @FXML
    private void clicBtnCancelar(ActionEvent event) {
        cerrarVentana();
    }
    
    private boolean validarCaracteres(String cadena){
        
        return cadena.matches("-?([a-zA-Z_0-9]*)?");
    }
    
     private boolean validarCaracteresEnteros(String cadena){
        
        return cadena.matches("\\d+");
    }
     
    private void cerrarVentana(){
       
        Stage escenarioRegistro = (Stage) tfClave.getScene().getWindow();
        escenarioRegistro.close(); 
   }
    
}