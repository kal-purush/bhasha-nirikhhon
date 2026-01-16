package object;

import entity.Entity;
import main.GamePanel;

public class OBJ_Metal_Chest extends Entity {
    GamePanel gp;
    Entity loot;
    boolean opened = false;

    public OBJ_Metal_Chest(GamePanel gp, Entity loot) {
        super(gp);
        this.gp = gp;
        this.loot = loot;

        type = type_obstacle;
        name = "Metal Chest";

        image = setup("/objects/Metalchest1", gp.tileSize, gp.tileSize);
        image2 = setup("/objects/Metalchest4",gp.tileSize, gp.tileSize);
        down1 = image;
        collision = true;

        solidArea.x = 4;
        solidArea.y = 16;
        solidArea.width = 40;
        solidArea.height = 32;
        solidAreaDefaultX = solidArea.x;
        solidAreaDefaultY = solidArea.y;
    }
    public void interact(){
        gp.gameState = gp.dialogueState;
        if(!opened){
            gp.ui.currentDialogue = "You need a metal key!!";
        }
        else {
            gp.ui.currentDialogue = "Its empty";
        }

    }
}