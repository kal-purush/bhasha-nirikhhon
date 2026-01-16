package abstractions;

import java.io.IOException;

import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.control.Slider;
import javafx.scene.layout.AnchorPane;

public abstract class OptionsPane {
	
	@FXML protected AnchorPane optionsPane;
	@FXML protected Slider diameterSlider;
	
	public OptionsPane(Parent parent, String fxmlPath) throws IOException {
		FXMLLoader loader = new FXMLLoader(getClass().getResource(fxmlPath));
		loader.setController(this);
		parent = loader.load();
	}
	
	public AnchorPane getOptionsPane() {
		return optionsPane;
	}
	
	public double getThicknessValue() {
		return diameterSlider.getValue();
	}
	
	public void disableSlider(boolean x) {
		diameterSlider.setDisable(x);
	}

}