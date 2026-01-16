package com.amproduction.amnews.util;

import javafx.scene.control.Alert;
import javafx.stage.Stage;

/**
 *  Created by snooki on 04.03.16.
 *  @version 1.0 2016-03
 *  @author Andrii Malchyk
 */
public class ShowAlert {

    private ShowAlert()
    {

    }
    public static void IOAlert(Stage aStage)
    {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.initOwner(aStage);
        alert.setTitle("AMNews");
        alert.setHeaderText("Помилка файлу конфігурації");
        alert.setContentText("Перевірте наявність файлу конфігурації");

        alert.showAndWait();
    }
}