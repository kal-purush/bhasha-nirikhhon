package com.qomtec.controllers;

import com.jfoenix.controls.JFXTextField;
import javafx.application.Application;
import javafx.event.Event;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;
import javafx.stage.StageStyle;

import java.io.IOException;

public class FrmPaciente extends Application {
    @FXML
    private JFXTextField txt_buscar;
    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {

    }
    @FXML
    protected void initialize(){
        txt_buscar.textProperty().addListener((obs, oldText, newText) -> {
            System.out.println(txt_buscar.getText());
        });


    }
    private void mostrarformulario(String formulario, String titulo){

        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource(formulario));
            Parent root1 = (Parent) loader.load();
            Scene scene = new Scene(root1);
            Stage stage = new Stage();
            stage.setScene(scene);
            stage.setTitle(titulo);
            stage.initStyle(StageStyle.UTILITY);
            stage.showAndWait();

        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }
    public void btn_buscarOnAction(Event e){

    }
    public void btn_nuevoOnAction(Event e){
        mostrarformulario("../frm_p_paciente.fxml","Paciente::.");
    }
    public void btn_modificarOnAction(Event e){
        mostrarformulario("../frm_p_paciente.fxml","Paciente::.");
    }
    public void btn_eliminarOnAction(Event e){

    }

}