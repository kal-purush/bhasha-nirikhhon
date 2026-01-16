package wastedgames.game.map;

import java.util.ArrayList;

public class Province {
    private ArrayList<Province> neighbours;
    private String name;
    private int level;


    public Province(ArrayList<Province> neighbours, String name, int level) {
        this.neighbours = neighbours;
        this.name = name;
        this.level = level;
    }

    public Province(ArrayList<Province> neighbours, String name) {
        this(neighbours, name, 1);
    }

    public Province(String name) {
        this(null, name, 1);
    }
}