public class SuperHero {

    public static void main(String[] args) {
	// write your code here

        /**

         ## Lesson 3 - Variables and Data Types

         - Create variables:
         - Hero name - String
         - Hero age - int
         - Hero powers - heroPower1, heroPower2, heroPower3
         - Create "Hero Card" view. Print variables to console

         */

        String nameOfHero;
        short ageOfHero;
        boolean isAwake;
        int netWorth;
        double averageWorthPerYear;
        float heightOfHero;
        char currencyOfNetWorth;
        byte gradeOfSuccess;
        String heroPower1, heroPower2, heroPower3;

        nameOfHero = "Java Guru";
        ageOfHero = 27;
        heightOfHero = 1.87F;
        isAwake = true;
        netWorth = 1000000;
        averageWorthPerYear = (double)netWorth / ageOfHero;
        currencyOfNetWorth = 'J';
        gradeOfSuccess = 100;

        heroPower1 = "Instantly knows right answer about any Java version and behavior in any place it is used!";
        heroPower2 = "Always awake! :) ";
        heroPower3 = "Detects syntax errors in all programming languages starting with letter \"J\".";

        System.out.println("************************************************************");
        System.out.println();
        System.out.println("Hero name: ");
        System.out.println(nameOfHero);
        System.out.print("Hero age: ");
        System.out.println(ageOfHero);
        System.out.println("Hero height: ");
        System.out.println(heightOfHero);

        System.out.println("------------");
        System.out.println();
        System.out.println("Hero powers:");
        System.out.println(heroPower1);
        System.out.println(heroPower2);
        System.out.println(heroPower3);

        System.out.println("------------");
        System.out.println();
        System.out.println("Net worth of nameOfHero " + nameOfHero + ": ");
        System.out.println(netWorth + "(" + currencyOfNetWorth + ")");

        System.out.println("------------");

        System.out.print("Is always awake? - ");
        System.out.println(isAwake);
        System.out.print("Grade of success (1 - 100): ");
        System.out.println(gradeOfSuccess);
        System.out.println("\n");
        System.out.println("************************************************************");
    }
}