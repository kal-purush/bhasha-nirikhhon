package de.hebk.game;

public class Highscore {
    private String name;
    private String gamemode;
    private int level;
    private int money;
    private String date;

    /**
     * Contructor for a Highscore
     * @param name      Name of the player
     * @param gamemode  Gamemode (Normal, Hardcore, True Or Not)
     * @param level     The level where the player has failed or finished
     * @param money     The amount of money the player has won
     * @param date      The date
     */
    public Highscore(String name, String gamemode, int level, int money, String date) {
        this.name = name;
        this.gamemode = gamemode;
        this.level = level;
        this.money = money;
        this.date = date;
    }

    /**
     * Gets the name of the player
     * @return  Name of the player
     */
    public String getName() {
        return this.name;
    }

    /**
     * Gets the gamemode the game has been played
     * @return  The gamemode
     */
    public String getGamemode() {
        return this.gamemode;
    }

    /**
     * Gets the level the player has failed or finished
     * @return  The level
     */
    public int getLevel() {
        return this.level;
    }

    /**
     * Gets the amount of money the player has won
     * @return  The amount of money
     */
    public int getMoney() {
        return this.money;
    }

    /**
     * Gets the date
     * @return  The date
     */
    public String getDate() {
        return this.date;
    }
}