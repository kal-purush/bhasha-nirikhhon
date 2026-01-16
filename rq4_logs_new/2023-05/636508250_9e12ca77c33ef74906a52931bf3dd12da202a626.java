package monster;

import entity.Entity;
import main.GamePanel;

public class MON_White extends Entity {
    GamePanel gp;
    int countTime = 0;

    public MON_White(GamePanel gp) {
        super(gp);

        this.gp = gp;

        type = type_monster;
        name = "White";
        defaultSpeed = 1;
        speed = defaultSpeed;
        maxLife = 5;
        life = maxLife;
        attack = 3;
        defense = 2;
        exp = 5;

        solidArea.x = 3;
        solidArea.y = 5;
        solidArea.width = 33;
        solidArea.height = 42;
        solidAreaDefaultX = solidArea.x;
        solidAreaDefaultY = solidArea.y;
        attackArea.width = 48;
        attackArea.height = 48;
        motion1_duration = 40;
        motion2_duration = 85;

        getImage();
        getAttackImage();
    }

    public void getImage() {
        up1 = setup("/monster/White_Idleleft1", gp.tileSize, gp.tileSize);
        up2 = setup("/monster/White_Idleleft2", gp.tileSize, gp.tileSize);
        down1 = setup("/monster/White_Idleright1", gp.tileSize, gp.tileSize);
        down2 = setup("/monster/White_Idleright2", gp.tileSize, gp.tileSize);
        left1 = setup("/monster/White_Idleleft1", gp.tileSize, gp.tileSize);
        left2 = setup("/monster/White_Idleleft2", gp.tileSize, gp.tileSize);
        right1 = setup("/monster/White_Idleright1", gp.tileSize, gp.tileSize);
        right2 = setup("/monster/White_Idleright2", gp.tileSize, gp.tileSize);
    }
    public void getAttackImage(){
        attackUp1 = setup("/monster/White_Attackleft1", gp.tileSize, gp.tileSize * 2);
        attackUp2 = setup("/monster/White_Attackleft2", gp.tileSize, gp.tileSize * 2);
        attackDown1 = setup("/monster/White_Attackright1", gp.tileSize, gp.tileSize * 2);
        attackDown2 = setup("/monster/White_Attackright2", gp.tileSize, gp.tileSize * 2);
        attackLeft1 = setup("/monster/White_Attackleft1", gp.tileSize * 2, gp.tileSize);
        attackLeft2 = setup("/monster/White_Attackleft2", gp.tileSize * 2, gp.tileSize);
        attackRight1 = setup("/monster/White_Attackright1", gp.tileSize * 2, gp.tileSize);
        attackRight2 = setup("/monster/White_Attackright2", gp.tileSize * 2, gp.tileSize);
    }


    public void setAction() {
        int xDistance = getXDistance(gp.player);
        int yDistance = Math.abs(worldY - gp.player.worldY);
        int titleDistance = (xDistance + yDistance)/gp.tileSize;

        if(onPath){
            //check if it stops chasing
            countTime++;
            if(titleDistance > 20){
                onPath = false;
                countTime = 0;
            }
            if(countTime > 120){
                onPath = false;
                countTime = 0;
            }
            //search the direction to chase
            searchPath(getGoalCol(gp.player), getGoalRow(gp.player));
        }
        else{
            //check if it starts chasing
            checkStartChasingOrNot(gp.player, 5, 100);

            //get a random direction
            getRamdomDirection();
        }

        //check if it attacks
        if(!attacking){
            checkAttackOrNot(30, gp.tileSize * 4, gp.tileSize);
        }
    }

    public void damageReaction() {
        actionLockCounter = 0;

//        direction = gp.player.direction;//make the monster go away from player
        onPath = true;
    }
}