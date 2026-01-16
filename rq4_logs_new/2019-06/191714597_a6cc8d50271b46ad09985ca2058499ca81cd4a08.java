import javafx.application.Application;
import javafx.scene.Group;
import javafx.scene.Scene;
import javafx.scene.paint.Color;
import javafx.scene.text.Text;
import javafx.stage.Stage;
import javafx.scene.shape.*;

public class QRLabel extends Application {
	//Generates and presents a QR label
	public void start(Stage primaryStage)
	{
		final int OUTER = 0;
		final int MID = 16;
		final int INNER = 32;
		final int SQUARES = 3;
		
		// initial values OUTER = 0, MID = 16, INNER = 32
		
		Rectangle outerRectangle = new Rectangle(OUTER, OUTER, 128, 128);
		outerRectangle.setFill(Color.BLACK);
		
		Rectangle midRectangle = new Rectangle(MID , MID, 96, 96);
		midRectangle.setFill(Color.WHITE);
		
		Rectangle innerRectangle = new Rectangle(INNER, INNER, 64, 64);
		innerRectangle.setFill(Color.BLACK);
		
		SQUARE Square1, Square2, Square3;
		Square1 = new SQUARE();
		Square2 = new SQUARE();
		Square3 = new SQUARE();
		
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}