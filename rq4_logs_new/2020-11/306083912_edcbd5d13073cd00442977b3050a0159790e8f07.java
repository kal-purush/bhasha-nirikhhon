package Homework_5;

import java.util.Random;

public abstract class Animals {

    private String animalType;
    private String animalName;
    private float runningDistance;
    private float swimmingDistance;
    private float jumpingHeight;
    protected final Random random = new Random();

    Animals(String animalType, String animalName, float runningDistance, float swimmingDistance, float jumpingHeight){
        this.animalType = animalType;
        this.animalName = animalName;
        this.runningDistance = runningDistance;
        this.swimmingDistance = swimmingDistance;
        this.jumpingHeight = jumpingHeight;
    }

    String getName(){
        return this.animalName;
    }

    float getSwimmingDistance(){
        return this.swimmingDistance;
    }

    protected boolean run(float distance){
        return distance <= (float) (random.nextFloat() * (this.runningDistance / 2) + this.runningDistance * 0.75);
    }

    protected boolean swim(float distance){
        return swimmingDistance != 0 &&
                distance <= (float) (random.nextFloat() * (this.swimmingDistance / 2) + this.swimmingDistance * 0.75);
    }

    protected boolean jump(float height){
        return height <= (float) (random.nextFloat() * (this.jumpingHeight / 2) + this.jumpingHeight * 0.75);
    }
}