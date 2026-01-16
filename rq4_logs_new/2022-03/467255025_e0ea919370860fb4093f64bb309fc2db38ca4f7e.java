public class myTestApp {

    static void randomNumberInOneOfThreeRanges(double number) {

        if (number <= 33) {
            System.out.println("number is higher then 0 but lesser then 33 --> " + number);
        }
        else if (number > 33 && number <= 66) {
            System.out.println("number is higher then 33 but lesser then 66 --> " + number);
        }
        else {
            System.out.println("number is higher then 66 but lesser then 100 -->  "+ number);
        }
    }

    static void chooseNumberToDisplayMonth(int number) {

        switch (number) {
            case 1 -> System.out.println(number + " chosen month is January");
            case 2 -> System.out.println(number + " chosen month is February");
            case 3 -> System.out.println(number + " chosen month is March");
            case 4 -> System.out.println(number + " chosen month is April");
            case 5 -> System.out.println(number + " chosen month is May");
            case 6 -> System.out.println(number + " chosen month is Jun");
            case 7 -> System.out.println(number + " chosen month is July");
            case 8 -> System.out.println(number + " chosen month is August");
            case 9 -> System.out.println(number + " chosen month is September");
            case 10 -> System.out.println(number + " chosen month is October");
            case 11 -> System.out.println(number + " chosen month is November");
            case 12 -> System.out.println(number + " chosen month is December");
            default -> System.out.println("You didn't choose number between 1 and 12");
        }

    }

    static void fillArrayWithNumbersFromOneToHundred(double[] arrayOfNumbers) {

        for (int i = 0; i < arrayOfNumbers.length; i++) {
            arrayOfNumbers[i] = Math.random() * 100;
            System.out.println("Number " + (i + 1) + "is : " + arrayOfNumbers[i]);
        }
    }

    static int sumAllNumbersFromArray(int[] arrayOfNumbers) {

        int result = 0;
        for (int numbers : arrayOfNumbers) {
            result += numbers;
        }
        return result;
    }

    static void displayFourSeasonUsingWhile() {

        int counter = 0;
        while (counter < 4) {

            if (counter == 0) {
                System.out.println("Spring!");
                counter++;
            } else if (counter == 1) {
                System.out.println("Summer!");
                counter++;
            } else if (counter == 2) {
                System.out.println("Autumn!");
                counter++;
            } else {
                System.out.println("Winter!");
                counter++;
            }
        }
    }

    static int sumOddNumbersFromArrayUsingWhile(int[] array) {

        int length = array.length;
        int counter = 0;
        int sum = 0;

        while (counter < length) {

            if (array[counter] % 2 == 0) {
                counter++;
                continue;

            } else {
                sum += array[counter];
                counter++;
            }
        }
        return sum;
    }

    static int sumOddNumbersFromArrayUsingFor(int[] array) {
        int sum = 0;
        for (int number : array) {
            if (number % 2 == 0) {
                continue;
            } else {
                sum += number;
            }
        }
        return sum;
    }

    static void countsFromGivenNumberToZero(int number) {

        do {
            if (number < 0){
                System.out.println("Please give number higher than 0!");
                break;
            }
            else if (number > 0) {
                System.out.println("Counting dwon!: " + number);
                number--;
            } else {
                System.out.println("Finish!: " + number);
                number--;
            }
        }

        while (number >= 0);
    }

    public static void main(String[] args){

        double randomNumber = Math.random()*100;
        double[] randomArray = new double[10];
        int[] integerArray = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        int[] integerArray2 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        //System.out.println("random number: "+randomNumber);

        System.out.println("\tTest If-else condition: ");
        System.out.println("Checks if number fits one of three ranges. 0-33, 34-66, 67-100");
        randomNumberInOneOfThreeRanges(randomNumber);
        System.out.println();

        System.out.println("\tTest Switch condition: ");
        System.out.println("Displays month according to given number");
        chooseNumberToDisplayMonth(2);
        chooseNumberToDisplayMonth(13);
        System.out.println();

        System.out.println("\tTest For loop: ");
        System.out.println("Fills array with ten (double) numbers form range 1 to 100");
        fillArrayWithNumbersFromOneToHundred(randomArray);
        System.out.println();

        System.out.println("\tTest For each loop: ");
        System.out.println("sum integer numbers from array");
        int result = sumAllNumbersFromArray(integerArray);
        System.out.println("The sum of numbers from 1 to 12: "+result);
        System.out.println();

        System.out.println("\tTest While loop: ");
        System.out.println("Displays names of season");
        displayFourSeasonUsingWhile();
        System.out.println();

        System.out.println("\tTest Do While loop: ");
        System.out.println("counts down form given number to zero");
        countsFromGivenNumberToZero(5);
        System.out.println();
        countsFromGivenNumberToZero(-2);
        System.out.println();

        System.out.println("\tTest While loop 2: ");
        int result2= sumOddNumbersFromArrayUsingWhile(integerArray2);
        System.out.println("gives the sum of odd numbers from array: "+result2);
        System.out.println();

        System.out.println("\tTest For loop 2: ");
        int result3= sumOddNumbersFromArrayUsingFor(integerArray2);
        System.out.println("gives the sum of odd numbers from array: "+result3);
        System.out.println();


    }
}