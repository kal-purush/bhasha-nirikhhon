package tictactoe.ui;

import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import tictactoe.view.RecordItem;

public class History extends ListView {

    public History() {

        setStyle("-fx-background-color: #1F509A; -fx-background-radius: 15;");

        ObservableList<RecordItem> records = FXCollections.observableArrayList(
                new RecordItem("Yousef", true),
                new RecordItem("Ahmed", false),
                new RecordItem("Ali", true)
        );

        getSelectionModel().selectedItemProperty().addListener(new ChangeListener() {
            @Override
            public void changed(ObservableValue observable, Object oldValue, Object newValue) {
                if (newValue != null) {
//                    TODO Handle record match
                }
            }
        });

        setCellFactory(value -> new ListCell<RecordItem>() {
            @Override
            protected void updateItem(RecordItem item, boolean empty) {
                super.updateItem(item, empty);

                if (item != null && !empty) {
                    setStyle("-fx-background-color: #fff;");
                    setGraphic(item);
                    
                } else {
                    setStyle("-fx-background-color: #1F509A;");
                    setGraphic(null);
                }
            }

        });

        setPrefHeight(500.0);
        setPrefWidth(350.0);
        setItems(records);
    }
}