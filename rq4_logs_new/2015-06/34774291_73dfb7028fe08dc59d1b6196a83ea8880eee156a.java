package gui.controller.dialog;

import javafx.fxml.FXML;
import javafx.scene.control.ComboBox;
import javafx.scene.control.TextField;
import javafx.scene.text.Text;

public class EnumerationDialogController extends DialogController {
   @FXML Text textBox;
   @FXML ComboBox comboBox;

   public void setText(String text) {
      this.textBox.setText(text);
   }

   public void setEnumerations(Object[] enumerations) {
      comboBox.getItems().clear();
      for (Object val : enumerations)
         comboBox.getItems().add(val);
   }

   @Override
   public void confirm() {
      setResponseValue(comboBox.getSelectionModel().getSelectedItem());
      close();
   }
}