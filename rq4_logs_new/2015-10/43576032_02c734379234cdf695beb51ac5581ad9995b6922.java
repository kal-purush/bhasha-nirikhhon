/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simon_7_methodmadnesspointillism;

import javafx.application.Application;
import javafx.scene.Group;
import javafx.scene.Scene;
import javafx.scene.canvas.Canvas;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.paint.Color;
import javafx.scene.shape.ArcType;
import javafx.stage.Stage;

/**
 *
 * @author asimon9159
 */
public class Simon_7_MethodMadnessPointillism extends Application {
    public static void main(String[] args) {
    launch(args);
    }
    
    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("Drawing Operations Test");
        Group root = new Group();
        Canvas canvas = new Canvas(300, 200) ;
        GraphicsContext gc = canvas.getGraphicsContext2D();
        drawBackground(gc,0,0,300,230);
        drawHead(gc);
        drawShell(gc);
        drawScales(gc);
        drawFeet(gc);
        drawEyes(gc);
        drawMouth(gc);
        drawHeadband(gc, 37, 85);
        //drawHeadbandBall (gc);
        drawEyebrow(gc);
        root.getChildren().add(canvas);
        primaryStage.setScene(new Scene(root));
        primaryStage.show();
    }
    
    private void drawBackground(GraphicsContext gc, int x, int y, int w, int h) {
    gc.setFill(Color.AQUA);
    gc.fillRect(x, y, w, h);
}

//private void drawShell(GraphicsContext gc) {
    //gc.setFill(Color.DARKGREEN);
    //gc.fillOval(90, 30, 150, 150);
//}

private void drawShell(GraphicsContext gc) {
    gc.setFill(Color.DARKGREEN);
    gc.fillArc(90, 30, 150, 200, 0, 180, ArcType.OPEN);
}

private void drawFeet(GraphicsContext gc) {
    gc.setFill(Color.GREEN);
    gc.fillArc(200, 120, 30, 30, 1, 240, ArcType.CHORD);
    gc.fillArc(100, 120, 30, 30, 270, 240, ArcType.CHORD);
}

private void drawScales(GraphicsContext gc) {
    gc.setFill(Color.BLACK);
    gc.setStroke(Color.BLACK);
    gc.setLineWidth(5);
    gc.strokeLine(95, 110, 120, 130);
    gc.strokeLine(109, 70, 190, 128);
    gc.strokeLine(135, 40, 235, 100);
    gc.strokeLine(200, 128, 230, 83);
    gc.strokeLine(150, 128, 200, 47);
    gc.strokeLine(100, 128, 150, 35);
}

private void drawHead(GraphicsContext gc) {
    gc.setFill(Color.GREEN);
    gc.fillOval(30, 70, 70, 70);
}

private void drawEyes(GraphicsContext gc) {
    gc.setFill(Color.BLACK);
    gc.fillOval(50, 90, 10, 10);
}

private void drawMouth(GraphicsContext gc) {
    gc.setFill(Color.BLACK);
    gc.setStroke(Color.BLACK);
    //gc.setLineWidth(5);     |     Line Width for the arc smile
    //gc.strokeLine(40, 120, 80, 140);     |    Arc for a smile?
    gc.fillOval(43, 110, 40, 20);
}

private void drawHeadband(GraphicsContext gc, int x, int y) {
    double[] xPoints = {x, x, x + 55 };
    double[] yPoints = {y, y - 10, y - 2.5};
    gc.setFill(Color.ORANGERED);
    gc.fillPolygon(xPoints, yPoints, 3);
    gc.fillOval(87, 77, 10, 10);
}

//private void drawHeadbandBall(GraphicsContext gc) {
    //gc.setFill(Color.ORANGERED);
    //gc.fillOval(87, 77, 10, 10);
//}

private void drawEyebrow(GraphicsContext gc) {
    //Worried Eyebrow or Angry Eyebrow?
    gc.setFill(Color.BLACK);
    gc.setStroke(Color.BLACK);
    gc.setLineWidth(5);
    //Below command "gc.strokeLine(40, 90, 70, 40);" is an angry japanese turtle
    gc.strokeLine(45, 90, 70, 40);
    //gc.strokeLine(40, 90, 70, 90);     |     For a sarcastic turtle
}

//IDEA: Air bubbles? Cat-Fish Whisker Plants? Lake shore?
//IDEA: Anime eyes?
//IDEA: NINJA TURTLES?!
//IDEA: Outline of whole turtle?

    }