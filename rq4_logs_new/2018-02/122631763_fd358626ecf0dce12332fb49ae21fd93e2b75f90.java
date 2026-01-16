package application;

import javafx.fxml.FXML;
import javafx.scene.canvas.Canvas;
import javafx.scene.canvas.GraphicsContext;

public class Image {
	@FXML private Canvas canvas;
	private GraphicsContext gc;
	Image image = null;

	public Image() {
		gc = canvas.getGraphicsContext2D();
		gc.clearRect(0, 0, canvas.getWidth(), canvas.getHeight());
		gc.drawImage(new Image(), 0, 0, );
	}

}