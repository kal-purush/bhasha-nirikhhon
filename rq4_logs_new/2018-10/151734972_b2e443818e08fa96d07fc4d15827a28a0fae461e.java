
public class GameState {

    private Cell sheepLocation, playerLocation, wolfLocation, shephardLocation, blockLocation;

    /**
     *
     */
    public GameState() {
        super();
    }

    public Cell getSheepLocation() {
        return sheepLocation;
    }

    public void setSheepLocation(Cell sheepLocation) {
        this.sheepLocation = sheepLocation;
    }

    public Cell getPlayerLocation() {
        return playerLocation;
    }

    public void setPlayerLocation(Cell playerLocation) {
        this.playerLocation = playerLocation;
    }

    public Cell getWolfLocation() {
        return wolfLocation;
    }

    public void setWolfLocation(Cell wolfLocation) {
        this.wolfLocation = wolfLocation;
    }

    public Cell getShephardLocation() {
        return shephardLocation;
    }

    public void setShephardLocation(Cell shephardLocation) {
        this.shephardLocation = shephardLocation;
    }

    public Cell getBlockLocation() {
        return blockLocation;
    }

    public static void setBlockLocation(Cell blockLocation) {
        blockLocation = blockLocation;
    }
}
