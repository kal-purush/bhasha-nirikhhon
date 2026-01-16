package javagame;

import org.newdawn.slick.*;
import org.newdawn.slick.geom.Vector2f;

import java.util.Vector;


public class Interaction {

    Player player;

    public Interaction() {
        player = player.getInstance();
    }

    public Vector<NPC> getLootableEnemies (float x, float y, int direction, NPC[] enemies) {

        // 0-Up, 1-Right, 2-Down, 3-Left

        Vector<NPC> lootableEnemies = new Vector<NPC>();

        for( NPC enemy : enemies ) {
            if( !enemy.getIsAlive() ) {
                    if ((enemy.getNPCX() >= x - 24 && enemy.getNPCX() <= x + 24) && (enemy.getNPCY() >= y - 24 && enemy.getNPCY() <= y + 24)) {
                        lootableEnemies.add(enemy);
                        System.out.println("\n");
                        enemy.getInventory().printInventory();
                    }

            }
        }

        return lootableEnemies;
    }

    public void selectOptions(Graphics g, Vector<NPC> enemies) {




    }

}