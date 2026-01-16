package ru.otus.java.basic.safarov.homework07;

public class Bicycle extends Transport implements MoveTransport {

    Human human;

    public Bicycle(String name, int maxResource, int consumption) {
        super(name, maxResource, consumption);
        this.typeTransort = "велосипед";
    }

    @Override
    public boolean move(int distance, Terrain terrain) {
        if (terrain == Terrain.SWAMP) {
            System.out.println(terrain + getMsgImposoble());
            return false;
        }
        boolean result = attemptMove(this, distance, terrain);
        human.setPower(currentResource);
        return result;
    }

    protected void setResource(Human human) {
        this.human = human;
        this.maxResource = human.getPower();
        this.currentResource = human.getPower();
    }

}