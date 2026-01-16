package opt3.Controller;

import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.layout.HBox;
import javafx.scene.text.Font;
import javafx.scene.text.Text;
import opt3.JavaApplication;
import opt3.Model.*;

import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.layout.AnchorPane;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;
import javafx.event.ActionEvent;
import javafx.scene.layout.VBox;

public class AddSortController {
    @FXML
    public TextField NameField;
    @FXML
    public CheckBox BrandField;
    @FXML
    public CheckBox WeightField;
    @FXML
    public CheckBox LoadField;
    @FXML
    public CheckBox TypeField;
    @FXML
    public TextField dayPrice;
    @FXML
    public TextField DL;
    @FXML
    public TextField IV;
    @FXML
    public TextField IW;

    public void Add(ActionEvent event) throws IOException {
        if (dayPrice.getText().trim().isEmpty())
            dayPrice = new TextField("0");
        if (DL.getText().trim().isEmpty())
            DL = new TextField("0");
        if (IV.getText().trim().isEmpty())
            IV = new TextField("0");
        if (IW.getText().trim().isEmpty())
            IW = new TextField("0");
        ItemSort sort = new ItemSort(NameField.getText(), BrandField.isSelected(), WeightField.isSelected(), LoadField.isSelected(), TypeField.isSelected(), Double.parseDouble(dayPrice.getText()), Double.parseDouble(DL.getText()), Double.parseDouble(IV.getText()), Double.parseDouble(IW.getText()));
        ItemSort.sorts.add(sort);
        ManageController.start(event);
    }
}