package hu.oe.nik.szfmv.automatedcar.model.objects;

import hu.oe.nik.szfmv.automatedcar.model.ScriptedPath;

public class Npc extends Collidable {

    public enum State {
        HEALTHY, DAMAGED, DEAD
    }

    private String healthyImageFileName;
    private String damagedImageFileName;
    private String deadImageFileName;

    private State state;
    private ScriptedPath path;

    public Npc(int x, int y, String healthyImageFileName, String damagedImageFileName, String deadImageFileName) {
        super(x, y, healthyImageFileName);

        this.state = State.HEALTHY;
        this.healthyImageFileName = healthyImageFileName;
        this.damagedImageFileName = damagedImageFileName;
        this.deadImageFileName = deadImageFileName;
    }

    public void HandleCollision() {
        if (this.state == State.HEALTHY) {
            this.state = State.DAMAGED;
            loadImage();
        } else if (this.state == State.DAMAGED) {
            this.state = State.DEAD;
            loadImage();
        }
    }

    private void loadImage() {
        switch (state) {
            case HEALTHY:
                this.imageFileName = healthyImageFileName;
                break;
            case DAMAGED:
                this.imageFileName = damagedImageFileName;
                break;
            case DEAD:
                this.imageFileName = deadImageFileName;
                break;
        }

        this.initImage();
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public ScriptedPath getPath() {
        return path;
    }

    public void setPath(ScriptedPath path) {
        this.path = path;
    }
}