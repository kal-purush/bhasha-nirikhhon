package pro.sky.java.course1.lesson7;

public class Program {
    public static void main(String[] args) {

        ////////// Task 1 //////////

        // Array 1
        int [] num = new int[3];
        num[0] = 1;
        num[1] = 2;
        num[2] = 3;

        // Array 2
        double [] numbers = {1.57, 7.654, 9.986};

        // Array 3
        char[] myChars = {'!', '?', '#'};


        ////////// Task 2 //////////
        System.out.println("Task 2:");

        // Array 1
        int lastIndexNum = num.length - 1;
        for (int i = 0; i < num.length; i++) {
            if (i == lastIndexNum) {
                System.out.print(num[i]  + "\n");
            } else {
                System.out.print(num[i] + ", ");
            }
        }

        // Array 2
        int lastIndexNumbers = numbers.length - 1;
        for (int i = 0; i < numbers.length; i++) {
            if (i == lastIndexNumbers) {
                System.out.print(numbers[i] + "\n");
            } else {
                System.out.print(numbers[i] + ", ");
            }
        }

        // Array 3
        int lastIndexMyChars = myChars.length - 1;
        for (int i = 0; i < myChars.length; i++) {
            if (i == lastIndexMyChars) {
                System.out.print(myChars[i]  + "\n");
            } else {
                System.out.print(myChars[i] + ", ");
            }
        }


        ////////// Task 3 //////////
        System.out.println("\nTask 3:");

        // Array 1
        for (int i = lastIndexNum; i >= 0; i--) {
            if (i == 0) {
                System.out.print(num[i]  + "\n");
            } else {
                System.out.print(num[i] + ", ");
            }
        }

        // Array 2
        for (int i = lastIndexNumbers; i >= 0; i--) {
            if (i == 0) {
                System.out.print(numbers[i]  + "\n");
            } else {
                System.out.print(numbers[i] + ", ");
            }
        }

        // Array 3
        for (int i = lastIndexMyChars; i >= 0; i--) {
            if (i == 0) {
                System.out.print(myChars[i]  + "\n");
            } else {
                System.out.print(myChars[i] + ", ");
            }
        }


        ////////// Task 4 //////////
        System.out.println("\nTask 4:");

        // Array 1
        for (int i = 0; i < num.length; i++) {
            if (num[i] % 2 != 0) {
                num[i] += 1;
            }
            if (i == lastIndexNum) {
                System.out.print(num[i]  + "\n");
            } else {
                System.out.print(num[i] + ", ");
            }
        }
    }
}