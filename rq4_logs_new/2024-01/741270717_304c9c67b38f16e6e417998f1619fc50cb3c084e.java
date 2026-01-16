package item.armor;

import item.base.BaseArmor;

public class Boots extends BaseArmor {
    // field
    private final int DEF;

    // constructor
    public Boots() {
        super("Boots", 5);
        this.DEF = 1;
    }

    // method
    public int getDef() {
        return DEF;
    }
}