package comp1110.ass2;
import javafx.animation.*;
import javafx.application.Application;
import javafx.beans.binding.Bindings;
import javafx.beans.property.IntegerProperty;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.event.Event;
import javafx.event.EventHandler;
import javafx.scene.Cursor;
import javafx.scene.Group;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.input.*;
import javafx.scene.layout.VBox;
import javafx.scene.layout.VBoxBuilder;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;
import javafx.scene.shape.Polygon;
import javafx.scene.text.Text;
import javafx.scene.transform.Rotate;
import javafx.stage.Stage;
import javafx.util.Duration;
import javafx.event.ActionEvent;

import java.awt.event.InputEvent;
import java.util.Random;


/**
 * Created by jieming on 2015/9/13.
 */



    /**
     * Created by jieming on 2015/8/8.
     */
    public class Board extends Application {
        final static int WIDTH=700;
        final static int HEIGHT=700;
        private IntegerProperty clickCounter=new SimpleIntegerProperty(this,"clickCounter");
        final Text source = new Text(50, 100, "DRAG ME");
        final Text target = new Text(300, 100, "DROP HERE");

        public void start(Stage primaryStage)throws Exception {
            HexGame hg=new HexGame();//create a Hexgame object
            Group root = new Group();

            int[] x = hg.minimalPath("171178187194205215093D038D064E070C100D043D106A108F072A080A051D112F082B016C118D060D125B122D093", 169, 93);
            String jj = Integer.toString(x.length);

            Scene scene = new Scene(root, WIDTH, HEIGHT);
            primaryStage.setTitle("HexGame");
            Polygon six1 = new Polygon();//create a polygen
            Button btn1=new Button();//create a button
            btn1.setText("Are you ready?");

            btn1.setOnAction(e -> {
                btn1.setText("Game start! Minimal Step is:" + jj);
            });

            //define a polygon
            six1.getPoints().addAll(new Double[]{
                    -100.0, 0.0,
                    -50.0, 86.6,
                    50.0, 86.6,
                    100.0, 0.0,
                    50.0, -86.6,
                    -50.0, -86.6,
            });
            //define the color of the ploygon
            six1.setFill(Color.GREEN);
                final Circle circle=new Circle(10,Color.GREEN);
            //mouse entered and exited color change
                six1.setOnMouseEntered(new EventHandler<MouseEvent>() {
                    @Override
                    public void handle(MouseEvent event) {
                        System.out.println("Mouse entered");
                        six1.setFill(Color.BLUE);
                        circle.setFill(Color.BLUE);
                    }
                });
            six1.setOnMouseExited(new EventHandler<MouseEvent>() {
                @Override
                public void handle(MouseEvent event) {
                    System.out.println("Mouse exited!");
                    six1.setFill(Color.GREEN);
                    circle.setFill(Color.GREEN);
                }
            });



            //six.setFill(colors[new Random().nextInt(colors.length)]);

            final Animation hitAnimation= SequentialTransitionBuilder.create().node(six1)
                    .children(
                            RotateTransitionBuilder.create()
                                    .fromAngle(0)
                                    .toAngle(1260)
                                    .duration(Duration.millis(800)).build()
                            //TranslateTransitionBuilder.create()
                            //.byY(1000).duration(Duration.millis(800)).build()
                    )
                    .build();

            six1.setOnMouseClicked(new EventHandler<MouseEvent>() {
                @Override
                public void handle(MouseEvent event) {
                    six1.setFill(Color.RED);
                    hitAnimation.play();
                    six1.setCursor(Cursor.CLOSED_HAND);
                    clickCounter.set(clickCounter.get() + 1);
                    circle.setTranslateX(350);
                    circle.setTranslateY(350);
                    six1.setOnMouseExited(new EventHandler<MouseEvent>() {
                        @Override
                        public void handle(MouseEvent event) {
                            System.out.println("Mouse exited!");
                            six1.setFill(Color.RED);
                        }
                    });
                }
            });


            final Text clickLabel = new Text();
            clickLabel.textProperty().bind(Bindings.concat(("Steps:"), clickCounter));
            final VBox hud=VBoxBuilder.create().children(clickLabel).translateX(50)
                    .translateY(60).build();

            int y=350;
            int h=350;

            six1.setTranslateX(h);
            six1.setTranslateY(y);
            btn1.setTranslateX(10);
            btn1.setTranslateY(10);

            root.getChildren().addAll(six1,btn1,circle,hud);
            primaryStage.setScene(scene);
            primaryStage.show();
        }

        public static void main(String[] args) {
           launch(args);
        }
}