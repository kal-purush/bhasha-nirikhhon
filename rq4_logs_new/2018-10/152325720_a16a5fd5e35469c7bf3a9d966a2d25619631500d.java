package ktu.tanks.builders;

import ktu.tanks.entities.Tank;

public abstract class TankBuilder {

    private Tank tank;

    abstract TankBuilder startNew();

    abstract TankBuilder addChassis();

    abstract TankBuilder addTracks();

    abstract TankBuilder addEngine();

    abstract TankBuilder addTurret();

    public Tank finish() {
        return tank;
    }
}