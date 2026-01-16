public class playerCharacter {
    // character stats

    // Define the stat names as constants
    private static final String STRENGTH = "Strength";
    private static final String LIFE = "Constitution";
    private static final String WEAPON_DAMAGE = "Weapon Damage";
    private static final String ARMOR_VALUE = "Armor Value";
    private static final String GOLD = "Gold";

    // Stats array to store the character's stats values
    public static double[] stats = {50, 300, 0, 0, 0};

    // Array to store descriptions for each stat
    private String[] statDescription = {STRENGTH, LIFE, WEAPON_DAMAGE, ARMOR_VALUE, GOLD};

    // Getter methods for the stats
    public static double getStrength() {
        return stats[0];
    }

    public static double getLife() {
        return stats[1];
    }

    public static double getWeaponDamage() {
        return stats[2];
    }

    public static double getArmorValue() {
        return stats[3];
    }

    public static double getGold() {
        return stats[4];
    }

    public static int attackDamage() {
        int damageDone = (int)Math.floor(getStrength()) + (int)Math.floor(getWeaponDamage());
        return damageDone;
    }
    // Setter methods
    public static void setStrength(double value) {
        stats[0] = value;
    }

    public static void setLife(double value) {
        stats[1] = value;
    }

    public static void setWeaponDamage(double value) {
        stats[2] = value;
    }

    public static void setArmorValue(double value) {
        stats[3] = value;
    }

    public static void addGold(double addAmount) {
        stats[4] = Math.max(0, stats[4] + addAmount);
    }

    public static void removeGold(double removeAmount) {
        stats[4] = Math.max(0, stats[4] - removeAmount);
    }

    // Calculate total damage output
    public static double calculateDamage() {
        return getStrength() + getWeaponDamage();
    }

    // Calculate total health
    public static double calculateHealth() {
        return getLife() * 5;
    }

    // Calculate damage taken after armor reduction
    public static double calculateDamageTaken (double incomingDamage) {
        return Math.max(0, incomingDamage - getArmorValue());
    }
}

/*
Stat Descriptions:
- Strength is the amount of damage you can do (1 str = 1 damage, 2 str = 2 damage, etc...)
- Life is the amount of health you have multiplied by 5 (1 life = 5 health, 2 life = 10 health, etc...)
- Weapon Damage is addition damage added onto your strength (1 weapon damage/1 str = 2 damage, 2 weapon damage/2 str = 4 damage, etc...)
- Armor Value is a flat reduction to damage taken (you take damage - armorValue = damageTaken)
- Gold is a resource that can be used to buy items
*/