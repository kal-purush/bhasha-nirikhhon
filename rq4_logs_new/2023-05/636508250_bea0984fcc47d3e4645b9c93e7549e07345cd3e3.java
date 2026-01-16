package object;

import entity.Entity;
import main.GamePanel;

public class OBJ_Devil_Sword extends Entity {

    public OBJ_Devil_Sword (GamePanel gp) {
        super(gp);

        type = type_sword;
        name = "Devil Sword";
        down1 = setup("/objects/sword_normal", gp.tileSize, gp.tileSize);
        attackValue = 3;
        attackArea.width = 36;
        attackArea.height = 40;
        description = "[" + name + "]\nAn sword of a forgotten devil.";
        knockBackPower = 8;
        bloodAbsorbment = 1;

        motion1_duration = 5;
        motion2_duration = 17;

    }
}