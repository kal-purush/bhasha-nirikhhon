
package inventarios;

import inventarios.util.Utilidades;
import java.io.IOException;
import java.net.URL;
import java.util.ResourceBundle;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import javafx.stage.Stage;

/**
 * FXML Controller class
 *
 * @author miran
 */
public class FXMLInicioSesionController implements Initializable {

    @FXML
    private PasswordField pfPassword;
    @FXML
    private TextField tfUsuario;
    @FXML
    private Label lbUsuarioError;
    @FXML
    private Label lbPasswordError;

    @Override
    public void initialize(URL url, ResourceBundle rb) {
    }    

    @FXML
    private void btnIniciarSesion(ActionEvent event) {
        String usuario = tfUsuario.getText();
        String password = pfPassword.getText();
        boolean valido = true;
        lbUsuarioError.setText("");
        lbPasswordError.setText("");
        if(usuario.isEmpty()){
            valido = false;
            lbUsuarioError.setText("Número de personal es requerido.");
        }
        if(password.isEmpty()){
            valido = false;
            lbPasswordError.setText("Contraseña es requerida.");
        }
        if(valido){
            verificarCredencialesUsuario(usuario, password);
        }
    }
    
    private void verificarCredencialesUsuario(String noPersonal, String password){
        try {
            
            if(noPersonal.contentEquals("1407")){
               irPantallaPrincipal("Diana");
            } else{
                Utilidades.mostrarAlertaSimple("Credenciales incorrectas", "El número de personal y/o contraseña"
                        + " es incorrecto, favor de verificar.", Alert.AlertType.WARNING);
            }
        } catch (NullPointerException e){
           Utilidades.mostrarAlertaSimple("Error de conexión", e.getMessage(), Alert.AlertType.ERROR);
        }
    }
    
    public void irPantallaPrincipal(String nombre){

        try {
            Utilidades.mostrarAlertaSimple("Bienvenido(a)", "Credenciales correctas, Bienvenido(a) al sistema "+nombre, Alert.AlertType.INFORMATION);
            Parent vista = FXMLLoader.load(getClass().getResource("FXMLPrincipal.fxml"));
            Scene escenaPrincipal = new Scene(vista);
            Stage escenarioBase = (Stage) tfUsuario.getScene().getWindow();
            escenarioBase.setTitle("Menú Principal");
            escenarioBase.setScene(escenaPrincipal);
            escenarioBase.show();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        
    }

    private void AND(boolean contentEquals) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
    
