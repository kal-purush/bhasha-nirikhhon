import processing.core.PApplet;

import java.util.ArrayList;
import java.util.Random;

public class GameMap {
    private PApplet p;
    private final int BLOCK_WIDTH = 100;
    private final int BLOCK_HEIGHT = 50;
    private ArrayList<MapBlock> blocks;

    public GameMap(PApplet pa) {
        this.p = pa;
        buildMap();
    }

    public void buildMap() {
        int currentX = 200;
        int currentY = p.height - 50;

        blocks = new ArrayList<MapBlock>();

        Random r = new Random();

        MapBlock block;
        int amountOfBlocks;

        for (int col = 0; col < 8; col++) {
            amountOfBlocks = r.nextInt(8) + 1;
            for (int level = 0; level < amountOfBlocks; level++) {
                block = new MapBlock(currentX, currentY, col);
                blocks.add(block);
                currentY -= BLOCK_HEIGHT;
            }
            currentY = p.height - 50;
            currentX += BLOCK_WIDTH;
        }
    }

    public void drawMap() {
        for (int i = 0; i < blocks.size(); i++) {
            MapBlock block = blocks.get(i);
            p.rect(block.x, block.y, BLOCK_WIDTH, BLOCK_HEIGHT);
        }
    }

    public ArrayList<MapBlock> getBlocks() {
        return blocks;
    }

    public int getBLOCK_WIDTH() {
        return BLOCK_WIDTH;
    }

    public int getBLOCK_HEIGHT() {
        return BLOCK_HEIGHT;
    }

    public void collapseColumn(int i) {
        MapBlock b = blocks.get(i);

        for (int j = 0; j < blocks.size(); j++) {
            MapBlock b2 = blocks.get(j);

            if (b2.col == b.col && b2.y < b.y) {
                b2.y += BLOCK_HEIGHT;
            }
        }

        blocks.remove(i);
    }
}