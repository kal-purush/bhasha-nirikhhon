import javafx.application.Application;
import javafx.scene.Group;
import javafx.scene.Scene;
import javafx.scene.shape.Circle;
import javafx.stage.Stage;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.control.Button;
import javafx.scene.paint.Color;
import javafx.scene.text.*;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;

public class HelloFX extends Application {
    // true = RED, false = YELLOW
    private boolean gameStatus = false;
    private boolean player = true;
    private Board state = new Board();
    private Circle[][] board = new Circle[6][7];
    // 0 = BLACK, 1 = YELLOW, 2 = RED
    // public HelloFX (int[][] state, boolean player) {
    //     this.state = state;
    //     this.player = player;
    // }

    public boolean changeCircle(int x, int y, boolean player, Text text) {
        if (player) {
            board[y][x].setFill(Color.RED);
            text.setText("Yellow Player's Turn");
        }
        else {
            board[y][x].setFill(Color.YELLOW);
            text.setText("Red Player's Turn");
        }
        int progress = state.checkWinner();
        if (progress != -1) {
            Alert a = new Alert(AlertType.NONE);
            a.setAlertType(AlertType.INFORMATION);
            String color = (progress == 2) ? "Red" : "Yellow";
            a.setContentText("Player " + color + " Wins!");
            a.show();
        }
        System.out.println("progress: " + progress);
        return !player;
    }

    public void start(Stage stage) {
        stage.sceneProperty().addListener((observable, oldScene, newScene) -> {
            System.out.println("New scene: " + newScene);
            System.out.println("Old scene: " + oldScene);
        });
        Group root = new Group();
        Text text = new Text();
        text.setX(310);
        text.setY(30);
        text.setText("Red Player's Turn");
        root.getChildren().add(text);
        for (int x = 0; x < 7; x++) {
            for (int y = 0; y < 6; y++) {
                Circle circ = new Circle(x * 100 + 60, y * 100 + 80, 30);
                int cellVal = state.getCell(x, y);
                if (cellVal == 0)
                    circ.setFill(Color.BLACK);
                else if (cellVal == 1)
                    circ.setFill(Color.RED);
                else
                    circ.setFill(Color.YELLOW);
                root.getChildren().add(circ);
                board[y][x] = circ;
            }
        
            Button btn = new Button();
            btn.setText("Column " + (x + 1));
            btn.setLayoutX(25 + 100 * x);
            btn.setLayoutY(640);
            btn.setId(Integer.toString(x));
            btn.setOnAction(new EventHandler<ActionEvent>() {
                @Override
                public void handle(ActionEvent event) {
                    int col = Integer.valueOf(btn.getId());
                    int val = (player ? 1 : 0) + 1;
                    try {
                        int[] c = state.insert(col, val);
                        player = changeCircle(c[0], c[1], player, text);
                    } catch (IllegalArgumentException e) {
                        System.out.println("ERROR");
                    }
                    System.out.println(state);
                }
            });
            root.getChildren().add(btn);
            if (gameStatus)
                btn.setDisable(true);
        }
        Scene scene = new Scene(root, 720, 680);
        scene.setFill(Color.BLUE);
        stage.setTitle("Connect 4");
        stage.setScene(scene);
        stage.show();
    }
}