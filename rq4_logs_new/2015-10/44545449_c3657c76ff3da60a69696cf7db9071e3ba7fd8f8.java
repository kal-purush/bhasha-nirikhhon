package hernandez_3_challengeone;
 
import javafx.application.Application;
import static javafx.application.Application.launch;
import javafx.scene.Group;
import javafx.scene.Scene;
import javafx.scene.canvas.Canvas;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.paint.Color;
import javafx.scene.shape.ArcType;
import javafx.stage.Stage;
 
public class Hernandez_3_challengeone extends Application {
 
    public static void main(String[] args) {
        launch(args);
    }
 
    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("Drawing Operations Test");
        Group root = new Group();
        Canvas canvas = new Canvas(300, 250);
        GraphicsContext gc = canvas.getGraphicsContext2D();
        drawShapes(gc);
        root.getChildren().add(canvas);
        primaryStage.setScene(new Scene(root));
        primaryStage.show();
    }
    private void drawHouse(GraphicsContext gc) {
        gc.fillText("my house", 42,42);
    }

    private void drawShapes(GraphicsContext gc) {
        gc.setFill(Color.INDIGO);
        gc.setStroke(Color.NAVY);
        gc.setLineWidth(2);
       
        gc.fillOval(5, 5, 67, 67);
        gc.strokeOval(80, 80, 55, 55);
        gc.fillOval(95, 45, 21, 21);
        gc.strokeOval(25, 75, 21, 21);
        gc.strokeOval(60, 160, 12, 12); 
        gc.strokeOval(130, 155, 45, 45);
        gc.fillOval(185, 195, 37, 37); 
        gc.strokeOval(65, 5, 10, 10);
        gc.fillOval(40, 190, 50, 50);
        gc.strokeOval(190, 59, 64, 64);
        gc.fillOval(170, 80, 10, 10);
        gc.fillOval(140, 40, 10, 10);
        gc.fillOval(90, 140, 12, 12);
        gc.fillOval(210, 150, 30, 30);
        gc.strokeOval(245, 200, 50, 50);
    }
}