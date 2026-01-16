package com.qomtec.controllers;

import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.qomtec.objects.Clinica;
import com.qomtec.utils.GenerateKey;
import com.qomtec.utils.Global;
import javafx.application.Application;
import javafx.event.Event;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;
import javafx.stage.Stage;

import java.net.URL;
import java.util.ResourceBundle;
import java.util.concurrent.CountDownLatch;

public class FrmClinica extends Application implements Initializable {
    @FXML private Button btn_cancelar;
    @FXML private TextField txt_codigo_clinica;
    @FXML private TextField txt_nombre_clinica;
    @FXML private TextField txt_propietario_clinica;
    @FXML private TextField txt_direccion_clinica;
    @FXML private TextField txt_telefono_clinica;
    @FXML private TextField txt_email_clinica;

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {

    }
    public void btn_guardarOnAction(Event e){
        Clinica clinica = new Clinica();
        clinica.setId_clinica(GenerateKey.getMD5(txt_email_clinica.getText()));
        clinica.setNombre_clinica(txt_nombre_clinica.getText());
        clinica.setNombre_propietario(txt_propietario_clinica.getText());
        clinica.setDireccion_clinica(txt_direccion_clinica.getText());
        clinica.setTelefono_clinica(txt_telefono_clinica.getText());
        clinica.setEmail_clinica(txt_email_clinica.getText());
        guardarClinica(clinica);
    }
    public void btn_cancelarOnAction(Event e){
        Stage stage = (Stage) btn_cancelar.getScene().getWindow();
        stage.close();
    }
    FirebaseDatabase db;
    public void guardarClinica(Clinica clinica){
        if (!clinica.getNombre_clinica().isEmpty()){
            db = FirebaseDatabase.getInstance();
            DatabaseReference tbl = db.getReference("tbl_clinica");
            DatabaseReference row = tbl.child(clinica.getId_clinica().substring(0,20));
            CountDownLatch count = new CountDownLatch(1);
            row.setValue(clinica, new DatabaseReference.CompletionListener() {
                @Override
                public void onComplete(DatabaseError databaseError, DatabaseReference databaseReference) {
                    System.out.println("Registro completo");
                }
            });
        }
    }

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        txt_email_clinica.setText(Global.ID_GENERAL);
    }
}