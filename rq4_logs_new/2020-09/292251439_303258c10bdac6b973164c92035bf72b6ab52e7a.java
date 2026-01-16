package sample;

import java.awt.*;
import java.io.IOException;
import java.net.URL;
import java.util.ConcurrentModificationException;
import java.util.ResourceBundle;

import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.*;
import javafx.scene.text.Text;
import javafx.stage.Stage;

import static javafx.scene.Cursor.*;

public class Controller {
    @FXML
    private ResourceBundle resources;
    @FXML
    private URL location;
    @FXML
    private AnchorPane root;
    ImageView gunJust = new ImageView("images/"+new Menu().graph+"/gunJust.png");
    ImageView gunStrike = new ImageView("images/"+new Menu().graph+"/gunStrike.png");
    ImageView ninja = new ImageView("images/"+new Menu().graph+"/ninja.gif");
    ImageView point = new ImageView("images/stop.png");
    ImageView imgDesc = new ImageView("images/lives.png");
    double ninX = 0;
    double ninY = 0;

    double ninUpX = 0; // верхний левый угол врага
    double ninUpY = 0;

    double ninDowX = 0; // нижний правый угол врага
    double ninDowY = 0;
    @FXML
    private ImageView back;

    @FXML
    private Text intStrikes;

    @FXML
    private Text strOver;

    @FXML
    private Text strOver2;
    @FXML
    private Text intLives;

    boolean thrStop = false;
    Integer lives = 6; // Жизни игрока
    Integer strikes = 0; // попадания игрока

    @FXML
    void initialize() throws IllegalArgumentException {
        back.setImage(new Image("images/"+new Menu().graph+"/back.png"));
            point.setY(65);
            point.setX(900);
            root.getChildren().add(point);

            imgDesc.setX(900);
            imgDesc.setY(12);
            root.getChildren().add(imgDesc);

        root.setOnMouseClicked(event -> { // метод попадания выстрелом
            ninUpX = ninX+211; // верхний левый угол врага
            ninUpY = ninY+214;

            ninDowX = ninX+258; // нижний правый угол врага
            ninDowY = ninY+372;

            double xG = gunJust.getX()+72; // координаты прицела Х
            double yG = gunJust.getY()-7; //  координаты прицела У

            if (xG>=ninUpX && xG<=ninDowX && yG>=ninUpY && yG<=ninDowY) { // попадание по врагу
                    thrStop = true;
            } // попадание
            else if (xG<1284.0 && xG>893.0 && yG<102.0 &&yG>59.0){ // закрытие окна по кнопке СТОП
                Stage stage = new Stage();
                Parent root2 = null;
                try {
                    root2 = FXMLLoader.load(getClass().getResource("fxml/Menu.fxml"));
                } catch (IOException e) { }
                stage.setTitle("Menu");
                stage.setScene(new Scene(root2, 590, 385));
                stage.setResizable(false);
                stage.show();
                root.getScene().getWindow().hide();
                lives = 0;
            }

        });

        back.setFitWidth(1500);

        new GunTh();
        new Enemy();
    }

    class GunTh extends Thread { ///////////////// Поток оружия
        GunTh() {
            start();
        }
        public void run() {
            root.setOnMouseMoved(event -> { // вождение пистолетом по экрану
                try {
                    root.setCursor(NONE); // курсор исчезает
                    gunJust.setX(MouseInfo.getPointerInfo().getLocation().x);
                    gunJust.setY(MouseInfo.getPointerInfo().getLocation().y);
                    root.getChildren().add(gunJust);
                }catch (IllegalArgumentException e) { }
            });

            root.setOnMousePressed(event -> { // нажатие на курок
                try {
                    root.setCursor(NONE); // курсор исчезает
                    gunStrike.setX(MouseInfo.getPointerInfo().getLocation().x);
                    gunStrike.setY(MouseInfo.getPointerInfo().getLocation().y);

                    root.getChildren().remove(gunJust);
                    root.getChildren().add(gunStrike);

                }catch (IllegalArgumentException e) {}
            });

            root.setOnMouseReleased(event -> { // отпуск курка
                try {
                root.getChildren().remove(gunStrike);
                root.setCursor(NONE); // курсор исчезает
                gunJust.setX(MouseInfo.getPointerInfo().getLocation().x);
                gunJust.setY(MouseInfo.getPointerInfo().getLocation().y);
                root.getChildren().add(gunJust);
            }catch (IllegalArgumentException e) { }
            });


        }
    }

    class Enemy extends Thread { //////////// поток противника
        Enemy() {
            start();
        }

        public void run() {
            try {
                ninja.setScaleY(0.5);
                ninja.setScaleX(0.4);
                ninja.setY(0);
                ninja.setX(1200);
                root.getChildren().add(ninja);

                for (; ;) {
                    intLives.setText(Integer.toString(lives)); // Шансы
                    countLivesAndStrikes();

                    //// перемещение вручную начало
                    ninja.setX(ninja.getX() - 10);
                    /// перемещение вручную конец
                    ninX = ninja.getX();
                    ninY = ninja.getY();

                    try {
                        if (strikes <= 1) Thread.sleep(35); // минимальная скорость 25 милс
                        else if (strikes <= 2) Thread.sleep(40);
                        else Thread.sleep(27);
                    } catch (InterruptedException e) {
                    } catch (ConcurrentModificationException e) {
                    }
                    if (lives <= 0) { // окончание жизней
                        strOver.setX(150);
                        strOver2.setX(135);
                        intStrikes.setX(-190);
                        intStrikes.setY(216);
                        strOver.setVisible(true);
                        strOver2.setVisible(true);
//                        intStrikes.setVisible(true);
                        intStrikes.setText(strikes.toString());
                        break;
                    }


                }
            } catch (NullPointerException e) {
                System.out.println("Неизвестная ошибка, тип 1");
            }
        }

        void countLivesAndStrikes() {
            try {
                double randY = Math.random() * 1000;
                double spawnY = 0;
                if (randY > 0 && randY < 400) spawnY = randY; // попадание рандомной координаты У в диапазон земли
                if (thrStop == false && ninja.getX() <= -300.0) {
                    lives = lives - 1;
                    intLives.setText(lives.toString());

                    ninja.setX(1200);
                    ninja.setY(spawnY);
                }
                if (thrStop == true) {
                    strikes = strikes + 1;
                    intStrikes.setVisible(true);
                    intStrikes.setText(strikes.toString());
                    ninja.setX(1200);
                    ninja.setY(spawnY);
                    thrStop = false;
                }


            } catch (NullPointerException e) {
                System.out.println("Неизвестная ошибка, тип 2");
            }
        }// вынести подсчеты в отдельный  метод чтобы разгрузить поток ПРотивника
    }
}



