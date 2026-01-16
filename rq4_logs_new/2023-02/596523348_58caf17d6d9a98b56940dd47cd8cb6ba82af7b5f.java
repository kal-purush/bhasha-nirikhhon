public class Game {
    private int level;
    private String nameVersion;
    private int timeOfGame;

    public Game(int level, String nameVersion, int timeOfGame) {
        this.level = level;
        this.nameVersion = nameVersion;
        this.timeOfGame = timeOfGame;
    }

    public int getLevel() {
        return level;
    }

    public String getNameVersion() {
        return nameVersion;
    }

    public int getTimeOfGame() {
        return timeOfGame;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public void setNameVersion(String nameVersion) {
        this.nameVersion = nameVersion;
    }

    public void setTimeOfGame(int timeOfGame) {
        this.timeOfGame = timeOfGame;
    }

    void  loadGame ( SavedVersion savedVersion) {
        this.level =savedVersion.getLevel();
        this.timeOfGame=savedVersion.getTimeOfGame();
        this.nameVersion=savedVersion.getNameVersion();

    }
   public SavedVersion saveGame() {
        return  new SavedVersion(this);
    }

    @Override
    public String toString() {
        return "Game{" +
                "level=" + level +
                ", nameVersion='" + nameVersion + '\'' +
                ", timeOfGame=" + timeOfGame +
                '}';
    }
}