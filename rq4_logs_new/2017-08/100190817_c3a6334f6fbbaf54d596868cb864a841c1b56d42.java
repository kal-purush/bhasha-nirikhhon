
public class Gravity {

    public static final double gravity = -9.81; // Earth's gravity in m/s^2

    // initialVelocity - starting velocity of the object
    // initialPosition - starting position in meters, the calculation will 	determine the ending position in meters
    // fallingTime - time in seconds that the object falls
    public double getFinalPosition(double fallingTime, double initialVelocity, double initialPosition) {
        double finalPosition = 0.0;
        if (fallingTime < 0)
            throw new ArithmeticException("fallingTime not positive");
        if (initialVelocity < 0)
            throw new ArithmeticException("initialVelocity not positive");
        if (initialPosition < 0)
            throw new ArithmeticException("initialPosition not positive");
        finalPosition = (gravity * (fallingTime * fallingTime) / 2) + (initialVelocity * fallingTime) + initialPosition;
        return finalPosition;
    }
}