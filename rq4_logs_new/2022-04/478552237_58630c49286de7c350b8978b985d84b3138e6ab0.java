import modele.Cell;
import modele.Direction;
import modele.Game;
import vue_controleur.Console2048;
import vue_controleur.Swing2048;

import java.awt.*;

public class Main {

    public static void main(String[] args) throws Exception {
        mainConsole();
        mainSwing();

        Game game = new Game(4);
        game.updateCell(new Cell(2), new Point(1,0));
        game.updateCell(new Cell(4), new Point(3,1));
        game.updateCell(new Cell(2), new Point(2,2));
        game.updateCell(new Cell(2), new Point(0,1));
        game.updateCell(new Cell(2), new Point(1,2));
        game.updateCell(new Cell(4), new Point(3,3));

        System.out.println(game.getCells().keySet());

        System.out.println(game);

        game.move(Direction.right);
        System.out.println(game.getCells().keySet());
        System.out.println(game);



        //System.out.println(game.getCells().keySet().toArray().length);

        //game.move(Direction.up);

        //System.out.println(game);





    }

    public static void mainConsole() {
        Game game = new Game(4);
        Console2048 vue = new Console2048(game);
        game.addObserver(vue);

        vue.start();

    }

    public static void mainSwing() {

        Game game = new Game(4);
        Swing2048 vue = new Swing2048(game);
        game.addObserver(vue);

        vue.setVisible(true);


    }



}