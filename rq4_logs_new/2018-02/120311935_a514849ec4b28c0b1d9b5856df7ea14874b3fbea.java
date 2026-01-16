package com.qomtec.controllers;

import com.qomtec.utils.Global;
import javafx.application.Application;
import javafx.event.Event;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;
import javafx.stage.StageStyle;

import java.io.IOException;

public class FrmOpciones {

    public void im_tratamiento_opcionesOnAction(Event e){
        Global.mostrarformulario("../views/frm_tipo_tratamiento.fxml","Tipos de tratamiento::.",getClass());
    }
}