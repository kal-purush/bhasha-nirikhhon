package hu.oe.nik.szfmv.automatedcar.virtualfunctionbus.packets;

public class CollisionPacket implements ReadOnlyCollisionPacket {

    private boolean collision = false;
    private float speedAfterCollision;
    private boolean gameOver = false;

    public boolean isCollision() {
        return collision;
    }

    public void setCollision(boolean collision) {
        this.collision = collision;
    }

    public float getSpeedAfterCollision() {
        return speedAfterCollision;
    }

    public void setSpeedAfterCollision(float speedAfterCollision) {
        this.speedAfterCollision = speedAfterCollision;
    }

    public boolean isGameOver() {
        return gameOver;
    }

    public void setGameOver(boolean gameOver) {
        this.gameOver = gameOver;
    }
}