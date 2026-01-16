package org.example;

public class TankControlSystem {
    private static final double MIN_LEVEL = 30.0;
    private static final double MAX_LEVEL = 100.0;

    private boolean pump; // Pump P status (true - ON, false - OFF)
    private boolean valveIn; // Filling valve status V1
    private boolean valveOut; // Filling valve status V2
    private double tankLevel; // Tank level LT1 (0-100%)
    private boolean drainBottom; //Drain button state (true - pressed)

    public TankControlSystem(double tankLevel) {
        this.valveIn = false;
        this.pump = false;
        this.valveOut = false;
        this.tankLevel = tankLevel;
        this.drainBottom = false;
    }

    public boolean isPump() {
        return pump;
    }

    public boolean isValveIn() {
        return valveIn;
    }

    public boolean isValveOut() {
        return valveOut;
    }

    public double getTankLevel() {
        return tankLevel;
    }

    public void openFillValve() {
        valveIn = true;
        System.out.println("Valve V1 is OPEN.");
    }

    public void closeFillValve() {
        valveIn = false;
        System.out.println("Valve V1 is CLOSE.");
    }

    public void setDrainBottom(boolean drainBottom) {
        this.drainBottom = drainBottom;
    }

    public void controlSystem() {
        if (drainBottom) {
            valveOut = true;
            valveIn = false;
        } else {
            valveOut = false;
        }

        if (valveIn) {
            if (tankLevel >= MIN_LEVEL && tankLevel < MAX_LEVEL) {
                pump = true;
            } else if (tankLevel >= MAX_LEVEL) {
                pump = false;
            }
        } else {
            pump = false;
        }

        printResult(MAX_LEVEL - tankLevel);
    }

    public void simulateStep(double levelChange) {
        if (pump) {
            tankLevel += levelChange;
        }
        if (valveOut) {
            tankLevel -= levelChange;
        }
        tankLevel = Math.max(0, Math.min(100, tankLevel));
    }

    private void printResult(double pumpTask) {
        System.out.println("----- System Status -----");
        System.out.println("Tank Level (LT1): " + tankLevel + "%");
        System.out.println("Pump Status: " + (pump ? "ON" : "OFF"));
        System.out.println("Valve V1 Status: " + (valveIn ? "OPEN" : "CLOSED"));
        System.out.println("Valve V2 Status: " + (valveOut ? "OPEN" : "CLOSED"));
        System.out.println("Level difference: " + pumpTask + "%");
        System.out.println("-------------------------\n");
    }
}