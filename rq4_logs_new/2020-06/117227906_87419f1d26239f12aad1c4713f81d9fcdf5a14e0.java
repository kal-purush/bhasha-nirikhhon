package learn.ijpds2nd.ch10oop;

/**
 * Body Mass Index.
 */
public class Bmi {
    private static final double KILOGRAMS_PER_POUND = 0.45359237;
    private static final double METERS_PER_INCH = 0.0254;

    private final Person person;
    private final double weightInPounds;
    private final double heightInInches;
    private final double bmi;
    private final String status;

    public Bmi(Person person, double weightInPounds, double heightInInches) {
        this.person = person;
        this.weightInPounds = weightInPounds;
        this.heightInInches = heightInInches;
        bmi = calculateBmi();
        status = calculateStatus();
    }

    public double getBmi() {
        return bmi;
    }

    public String getStatus() {
        return status;
    }

    public Person getPerson() {
        return (Person) person.clone();
    }

    private double calculateBmi() {
        double weightInKilograms = weightInPounds * KILOGRAMS_PER_POUND;
        double heightInMeters = heightInInches * METERS_PER_INCH;
        return weightInKilograms / (heightInMeters * heightInMeters);
    }

    private String calculateStatus() {
        if (bmi < 18.5) {
            return "Underweight";
        } else if (bmi < 25) {
            return "Normal";
        } else if (bmi < 30) {
            return "Overweight";
        } else {
            return "Obese";
        }
    }
}