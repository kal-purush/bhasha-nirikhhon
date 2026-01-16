class Answer {

    // Change these boolean values to control whether you see 
    // the expected result and/or hints.
    static boolean showExpectedResult = true;
    static boolean showHints = true;

    static double calculateYearlySalary(double hoursPerWeek, double amountPerHour) {
        // Your code goes here.
        double result = hoursPerWeek * amountPerHour * 52;

        return result;
    }

}