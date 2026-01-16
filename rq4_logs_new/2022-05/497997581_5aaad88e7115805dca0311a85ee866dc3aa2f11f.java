/*package gamestudio;

import gamestudio.game.core.Boxes;
import gamestudio.game.core.Field;
import gamestudio.game.consoleui.ConsoleUI;
import gamestudio.service.CommentServiceJDBC;
import gamestudio.service.RatingServiceJDBC;
import gamestudio.service.ScoreServiceJDBC;

public class Main {
    public static void main(String[] args) {
        Boxes boxes = new Boxes();
        boxes.UpdateBoxes();


        Field field = new Field(boxes);


        ConsoleUI ui = new ConsoleUI(field,new ScoreServiceJDBC(),new RatingServiceJDBC(), new CommentServiceJDBC());

        ui.Play(field, boxes);
        // boxes.getPrintingField();
    }
}*/