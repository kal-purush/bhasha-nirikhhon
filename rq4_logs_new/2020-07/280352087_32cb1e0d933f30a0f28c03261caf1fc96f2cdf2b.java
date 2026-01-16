package wastedgames.game.map;

public abstract class Unit {
    private UnitType unitType;

    private int health;
    private int power;
    private int speed;
    private int price;
    private int level;
    private int armor;

    public enum UnitType {
        INFANTRY, CAVALRY
    }

    public Unit(UnitType unitType, int health, int power, int speed, int price, int armor, int level) {
        this.unitType = unitType;
        this.health = health;
        this.power = power;
        this.speed = speed;
        this.price = price;
        this.level = level;
        this.armor = armor;
    }

    public int getHealth() {
        return health;
    }

    public int getPower() {
        return power;
    }

    public int getPrice() {
        return price;
    }

    public int getSpeed() {
        return speed;
    }

    public UnitType getUnitType() {
        return unitType;
    }

    public int getLevel() {
        return level;
    }

    public int getArmor() {
        return armor;
    }

    protected void getDamage(int damage) {
        this.health -= damage / armor;
    }

    protected void getHeal(int heal) {
        this.health += heal;
    }
}