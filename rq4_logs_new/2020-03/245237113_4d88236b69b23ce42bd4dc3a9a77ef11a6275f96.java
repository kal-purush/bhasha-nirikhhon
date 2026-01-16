package sample;

import javafx.application.Application;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.layout.StackPane;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.Text;
import javafx.stage.Stage;

import java.util.Scanner;

public class Main extends Application {

    @Override
    public void start(Stage primaryStage) throws Exception{
        Scanner in = new Scanner(System.in);
        StackPane root = new StackPane();
        Scene scene = new Scene(root,300,275);
        primaryStage.setTitle("Hello World");
        primaryStage.setScene(scene);
        primaryStage.show();
        String s = in.nextLine();

        Text t = new Text(s);
        t.setFont(Font.font("Calibri",20));
        root.getChildren().addAll(t);
        int a=0;
        int c=0;

        int r;
        int g;
        int b;

        for (int i=0;i<s.length();i++) {
            switch (s.toLowerCase().substring(i,i+1)) {
                case "a":
                case "e":
                case "i":
                case "o":
                case "u":
                case "á":
                case "é":
                case "í":
                case "ó":
                case "ú":
                case "ä":
                case "ô":
                    a++;
                default:
                    c++;
            }
        }
            if ((a*16)<=255) {
                r = a*16;
            } else {
                r = 0;
            }

            if ((c*24)<=255) {
                g = c*24;
            } else {
                g = 0;
            }

            if ((((a+c)%25)*10)<=255) {
                b = (((a+c)%25)*10);
            } else {
                b = 0;
            }
        scene.setFill(Color.rgb(r,g,b));
    }


    public static void main(String[] args) {
        launch(args);
    }
}