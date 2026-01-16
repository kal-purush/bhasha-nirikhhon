package com.mvelyka.ocw;

public class Gravity {

    public static final double gravity = -9.81; // Earth's gravity in m/s^2

    /**
     * Gets final position of falling object.
     *
     * @param initialVelocity Starting velocity of the object
     * @param initialPosition Starting position in meters, the calculation will 	determine the ending position in meters
     * @param fallingTime     Time in seconds that the object falls
     * @return finalPosition of falling object
     */
    public double getFinalPosition(double fallingTime, double initialVelocity, double initialPosition) {
        double finalPosition;

        validatePositive(fallingTime);
        validatePositive(initialVelocity);
        validatePositive(initialPosition);

        finalPosition = (gravity * (fallingTime * fallingTime) / 2) + (initialVelocity * fallingTime) + initialPosition;

        return finalPosition;
    }

    /**
     * Validates if input param is positive value.
     * redo
     */
    public void validatePositive(double value) {

        if (value < 0)
            throw new ArithmeticException("Parameter is not positive");

    }
}