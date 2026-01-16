package game;


import game.moduls.Card;
import game.moduls.Picture;
import game.moduls.Player;
import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;
import javafx.stage.StageStyle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by Rustymattok on 25.10.2015.
 */
//todo Выяснить почему не могу вызвать css из папки css- что то с путем указания.
public class MainApp extends Application {

    private static Stage primarystage;
    private static Stage stage;
    private static BorderPane rootpane;
    private static Card card;
    private static Card cardPlayer;
    private static Card allPlayer;
    private static ArrayList<Card> listAll = new ArrayList<>();
    private static ArrayList<Card> list = new ArrayList<>();
    private static ArrayList<Card> listplayer = new ArrayList<>();
    private static Player player;
    private static Player alPlayer;
    static int num = 0;
    private static int result = 0;

    @Override
    public void start(Stage primaryStage) throws Exception {
        this.primarystage = primaryStage;
        showMainMenu();

    }

    //Вывод на экран основного меню
    public void showMainMenu() {
        try {
            FXMLLoader loader = new FXMLLoader();
            loader.setLocation(MainApp.class.getResource("view/mainMenu.fxml"));
            rootpane = loader.load();
            Scene scene = new Scene(rootpane);
            scene.getStylesheets().add(0, "game/style.css");
            primarystage.initStyle(StageStyle.TRANSPARENT);
            primarystage.setTitle("Game");
            primarystage.setScene(scene);
            primarystage.show();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // вывод на экран меню управления игры и перемешанной колоды карт
    public static void showPlayWindow() {
        try {
            FXMLLoader loader = new FXMLLoader();
            loader.setLocation(MainApp.class.getResource("view/gameMenu.fxml"));
            AnchorPane pane = loader.load();
            rootpane.setLeft(pane);
            for (int i = 0; i < 36; i++) {
                card = new Card(new Picture(), i + 1);
                card.getPicture().getImageView().setTranslateX(i * 1);
                list.add(card);
                rootpane.getChildren().addAll(card);
            }
//            System.out.println(list.get(1).getPicture().getNumber());
            Collections.shuffle(list);
//            System.out.println(list.get(1).getPicture().getNumber());
//            System.out.println(showList(list));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void showExitWindow() {

        System.exit(0);
    }

    // возрат в исходное меню в случае отмены
    public static void showMenuWindow() {
        try {
            FXMLLoader loader = new FXMLLoader();
            loader.setLocation(MainApp.class.getResource("view/mainMenu.fxml"));
            rootpane = loader.load();
            list.removeAll(list);
            listplayer.removeAll(listplayer);
            listAll.removeAll(listAll);
            num = 0;
            Scene scene = new Scene(rootpane);
            scene.getStylesheets().add(0, "game/style.css");
            primarystage.setScene(scene);
            primarystage.show();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //обработчки приемки карты
    public static void showTakeCardButton() throws IOException {

        cardPlayer = new Card(new Picture(list.get(list.size() - 1).getPicture().getNumber()), list.get(list.size() - 1).getPicture().getNumber());
        listplayer.add(cardPlayer);
        result = listplayer.get(listplayer.size() - 1).getPicture().getValue() + result;
//        System.out.println(" my count = " + result);
        rootpane.getChildren().remove(list.get(list.size() - 1));
        list.remove(list.size() - 1);
        listplayer.get(listplayer.size() - 1).getPicture().getImageView().setTranslateX(num * 50);
        num++;
        AnchorPane pane = new AnchorPane();
        rootpane.setBottom(pane);
        pane.getChildren().addAll(listplayer);
        pane.setTranslateX(10);
    }

    //карты берет компьютер, функция отправляется в контроллер по нажатию клавиши let's
    public static void setCardsAll() {
        player = new Player(listplayer, result);
        result = 0;
        int i = 0;
        do {
            allPlayer = new Card(new Picture(list.get(list.size() - 1).getPicture().getNumber()), list.get(list.size() - 1).getPicture().getNumber());
            listAll.add(allPlayer);
            list.remove(list.size() - 1);
            listAll.get(listAll.size() - 1).getPicture().getImageView().setTranslateY(i * 50);
            listAll.get(listAll.size() - 1).getPicture().getImageView().setRotate(270);
            listAll.get(listAll.size() - 1).getPicture().getImageView().setTranslateX(-20);
            i++;
            result = listAll.get(listAll.size() - 1).getPicture().getValue() + result;
//            System.out.println(showList(listAll));
            AnchorPane pane = new AnchorPane();
            rootpane.setRight(pane);
            pane.getChildren().addAll(listAll);
            pane.setTranslateY(200);
        } while (result <= 17);
        alPlayer = new Player(listAll, result);
        result = 0;
//        System.out.println("Количесво очков компьюетеры  " + alPlayer.getResult());
    }

    //метод определяющи  кто выйграл
    //todo подумать над алгоритмом проверки
    public static String result(String s) {
//        player = new Player(listplayer, result);
        s = "";
        int a = player.getResult();
        int b = alPlayer.getResult();
        if (a == b) {
            s = s + "ничья";
            return s;
        }
        if (a > 21)
            if (b > 21) {
                s = s + "ничья";
            } else {
                s = s + "поражение";
                return s;
            }
        if (a < 21)
            if (b > 21) {
                s = s + "победа";
            } else if (b < 21)
                if (a > b) {
                    s = s + "победа";
                } else {
                    s = s + "поражение";
                }
        if (a == 21)
            s = s + "победа";
        else if (b == 21) {
            s = s + "поражение";
        }

        return s;
    }
    public static String showResult() {
        String s = "";
        s = s + "Pl : " +  player.getResult() + "/" + alPlayer.getResult() + " : ALL";
        return s;
    }


    //методы для тестирования--------------------->
    public static void showPushLets() throws IOException {

        player = new Player(listplayer, result);
    }

    public static Card getCard() {
        return card;
    }

    public static ArrayList<Card> getList() {
        return list;
    }

    public static Stage getStage() {
        return stage;
    }

    public static Player getPlayer() {
        return player;
    }

    public static Player getAlPlayer() {
        return alPlayer;
    }


    public static ArrayList<Card> getListplayer() {
        return listplayer;
    }


    public static String showList(ArrayList<Card> list) {
        String s = " ";
        for (Card x : list) {
            s = s + x.getPicture().getNumber() + " ";
        }
        return s;
    }
}