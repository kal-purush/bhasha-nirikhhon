package com.jetbrains;

import java.util.Random;
import java.util.Scanner;
//final
public class Main
{


    public static void Add(int counter, int rightCounter, int diffLvl, int opLvl)
    {

        if(counter == 10)
        {
            double avg = (double)((rightCounter/counter) * 100);
            if(avg >= 75)
            {
                System.out.println("Congratulations, you are ready to go to the next level!");
                return;
            }
            else
            {
                System.out.println("Please ask your teacher for extra help.");
                return;
            }
        }

        Scanner scnr = new Scanner(System.in);
        Random SecureRandom = new Random();
        int num1;
        int num2;
        if(diffLvl == 1)
        {
            num1 = SecureRandom.nextInt(10);
            num2 = SecureRandom.nextInt(10);
        }
        else if(diffLvl == 2)
        {
            num1 = SecureRandom.nextInt(21);
            num2 = SecureRandom.nextInt(21);
        }
        else if(diffLvl == 3)
        {
            num1 = SecureRandom.nextInt(101);
            num2 = SecureRandom.nextInt(101);
        }
        else if(diffLvl == 4)
        {
            num1 = SecureRandom.nextInt(1001);
            num2 = SecureRandom.nextInt(1001);
        }
        else
        {
            System.out.println("INVALID INPUT");
            return;
        }

        System.out.println("How much is " + num1 + " plus " + num2 + "?");
        double userAnswer = scnr.nextDouble();
        int temp = 0;
        while(userAnswer != (num1 + num2))
        {
            if(temp == 1)
                break;

            temp++;
            int output = SecureRandom.nextInt(5);
            switch (output)
            {
                case 1:
                    System.out.println("No. Please try again.");
                    break;
                case 2:
                    System.out.println("Wrong. Try once more.");
                    break;
                case 3:
                    System.out.println("Don’t give up!");
                    break;
                case 4:
                    System.out.println("No. Keep trying.");
                    break;
            }
            userAnswer = scnr.nextDouble();
        }
        counter++;
        if(userAnswer == (num1 + num2))
        {
            rightCounter++;
        }
        if (counter == 10)
            return;

        Add(counter,rightCounter,diffLvl,opLvl);

        if (counter == 10)
            return;
    }

    public static void Mul(int counter, int rightCounter, int diffLvl, int opLvl)
    {
        if(counter == 10)
        {
            double avg = (double)((rightCounter/counter) * 100);
            if(avg >= 75)
                System.out.println("Congratulations, you are ready to go to the next level!");
            else
            {
                System.out.println("Please ask your teacher for extra help.");
                counter = 0;
                rightCounter = 0;
                //Mul(rightCounter,counter,diffLvl,opLvl);
            }
            return;
        }
        //checkDone(counter,rightCounter,diffLvl,opLvl);
        Scanner scnr = new Scanner(System.in);
        Random SecureRandom = new Random();
        int num1;
        int num2;
        if(diffLvl == 1)
        {
            num1 = SecureRandom.nextInt(10);
            num2 = SecureRandom.nextInt(10);
        }
        else if(diffLvl == 2)
        {
            num1 = SecureRandom.nextInt(21);
            num2 = SecureRandom.nextInt(21);
        }
        else if(diffLvl == 3)
        {
            num1 = SecureRandom.nextInt(101);
            num2 = SecureRandom.nextInt(101);
        }
        else if(diffLvl == 4)
        {
            num1 = SecureRandom.nextInt(1001);
            num2 = SecureRandom.nextInt(1001);
        }
        else
        {
            System.out.println("INVALID INPUT");
            return;
        }

        System.out.println("How much is " + num1 + " times " + num2 + "?");
        double userAnswer = scnr.nextDouble();
        int temp = 0;
        while(userAnswer != (num1 * num2))
        {
            if(temp == 1)
                break;

            temp++;
            int output = SecureRandom.nextInt(5);
            switch (output)
            {
                case 1:
                    System.out.println("No. Please try again.");
                    break;
                case 2:
                    System.out.println("Wrong. Try once more.");
                    break;
                case 3:
                    System.out.println("Don’t give up!");
                    break;
                case 4:
                    System.out.println("No. Keep trying.");
                    break;
            }
            userAnswer = scnr.nextInt();
        }
        counter++;
        if (counter == 10)
            return;
        if(userAnswer == (num1 * num2))
        {
            rightCounter++;
        }

        Mul(counter,rightCounter,diffLvl,opLvl);
    }

    public static void Sub(int counter, int rightCounter, int diffLvl, int opLvl)
    {
        if(counter == 10)
        {
            double avg = (double)((rightCounter/counter) * 100);
            if(avg >= 75)
                System.out.println("Congratulations, you are ready to go to the next level!");
            else
            {
                System.out.println("Please ask your teacher for extra help.");
                counter = 0;
                rightCounter = 0;
                //Sub(rightCounter,counter,diffLvl,opLvl);
            }
            return;
        }
        //checkDone(counter,rightCounter,diffLvl,opLvl);
        Scanner scnr = new Scanner(System.in);
        Random SecureRandom = new Random();
        int num1;
        int num2;
        if(diffLvl == 1)
        {
            num1 = SecureRandom.nextInt(10);
            num2 = SecureRandom.nextInt(10);
        }
        else if(diffLvl == 2)
        {
            num1 = SecureRandom.nextInt(21);
            num2 = SecureRandom.nextInt(21);
        }
        else if(diffLvl == 3)
        {
            num1 = SecureRandom.nextInt(101);
            num2 = SecureRandom.nextInt(101);
        }
        else if(diffLvl == 4)
        {
            num1 = SecureRandom.nextInt(1001);
            num2 = SecureRandom.nextInt(1001);
        }
        else
        {
            System.out.println("INVALID INPUT");
            return;
        }

        System.out.println("How much is " + num1 + " minus " + num2 + "?");
        double userAnswer = scnr.nextDouble();
        int temp = 0;
        while(userAnswer != (num1 - num2))
        {
            if(temp == 1)
                break;

            temp++;
            int output = SecureRandom.nextInt(5);
            switch (output)
            {
                case 1:
                    System.out.println("No. Please try again.");
                    break;
                case 2:
                    System.out.println("Wrong. Try once more.");
                    break;
                case 3:
                    System.out.println("Don’t give up!");
                    break;
                case 4:
                    System.out.println("No. Keep trying.");
                    break;
            }
            userAnswer = scnr.nextDouble();
        }
        counter++;
        if (counter == 10)
            return;

        if(userAnswer == (num1 - num2))
        {
            rightCounter++;
        }

        Sub(counter,rightCounter,diffLvl,opLvl);
    }

    public static void Div(int counter, int rightCounter, int diffLvl, int opLvl)
    {
        if(counter == 10)
        {
            double avg = (double)((rightCounter/counter) * 100);
            if(avg >= 75)
                System.out.println("Congratulations, you are ready to go to the next level!");
            else
            {
                System.out.println("Please ask your teacher for extra help.");
                counter = 0;
                rightCounter = 0;
                //Div(rightCounter,counter,diffLvl,opLvl);
            }
            return;
        }
        //checkDone(counter, rightCounter, diffLvl, opLvl);
        Scanner scnr = new Scanner(System.in);
        Random SecureRandom = new Random();
        int num1;
        int num2;
        if(diffLvl == 1)
        {
            num1 = SecureRandom.nextInt(10);
            num2 = SecureRandom.nextInt(10);
        }
        else if(diffLvl == 2)
        {
            num1 = SecureRandom.nextInt(21);
            num2 = SecureRandom.nextInt(21);
        }
        else if(diffLvl == 3)
        {
            num1 = SecureRandom.nextInt(101);
            num2 = SecureRandom.nextInt(101);
        }
        else if(diffLvl == 4)
        {
            num1 = SecureRandom.nextInt(1001);
            num2 = SecureRandom.nextInt(1001);
        }
        else
        {
            System.out.println("INVALID INPUT");
            return;
        }

        System.out.println("How much is " + num1 + " / " + num2 + "?");
        double userAnswer = scnr.nextDouble();
        int temp = 0;
        while(userAnswer != (num1 / num2))
        {
            if(temp == 1)
                break;

            temp++;
            int output = SecureRandom.nextInt(5);
            switch (output)
            {
                case 1:
                    System.out.println("No. Please try again.");
                    break;
                case 2:
                    System.out.println("Wrong. Try once more.");
                    break;
                case 3:
                    System.out.println("Don’t give up!");
                    break;
                case 4:
                    System.out.println("No. Keep trying.");
                    break;
            }
            userAnswer = scnr.nextDouble();
        }
        counter++;

        if (counter == 10)
            return;

        if(userAnswer == (num1 / num2))
        {
            rightCounter++;
        }

        Div(counter,rightCounter,diffLvl,opLvl);
    }

    public static void mix(int counter, int rightCounter, int diffLvl, int opLvl)
    {
        if(counter == 10)
        {
            double avg = (double)((rightCounter/counter) * 100);
            if(avg >= 75)
                System.out.println("Congratulations, you are ready to go to the next level!");
            else
            {
                System.out.println("Please ask your teacher for extra help.");
                counter = 0;
                rightCounter = 0;
                Add(rightCounter,counter,diffLvl,opLvl);
            }
            return;
        }

        Scanner scnr = new Scanner(System.in);
        Random SecureRandom = new Random();
        int num1;
        int num2;
        if(diffLvl == 1)
        {
            num1 = SecureRandom.nextInt(10);
            num2 = SecureRandom.nextInt(10);
        }
        else if(diffLvl == 2)
        {
            num1 = SecureRandom.nextInt(21);
            num2 = SecureRandom.nextInt(21);
        }
        else if(diffLvl == 3)
        {
            num1 = SecureRandom.nextInt(101);
            num2 = SecureRandom.nextInt(101);
        }
        else if(diffLvl == 4)
        {
            num1 = SecureRandom.nextInt(1001);
            num2 = SecureRandom.nextInt(1001);
        }
        else
        {
            System.out.println("INVALID INPUT");
            return;
        }
        int op = SecureRandom.nextInt(5);
        switch(op)
        {
            case 1:
                Add(counter, rightCounter, diffLvl, opLvl);
            case 2:
                Mul(counter, rightCounter, diffLvl, opLvl);
            case 3:
                Sub(counter, rightCounter, diffLvl, opLvl);
            case 4:
                Div(counter, rightCounter, diffLvl, opLvl);
            case 5:
                mix(counter, rightCounter, diffLvl, opLvl);

        }
        System.out.println("How much is " + num1 + " plus " + num2 + "?");
        Double userAnswer = scnr.nextDouble();
        int temp = 0;
        while(userAnswer != (num1 + num2))
        {
            if(temp == 1)
                break;

            temp++;
            int output = SecureRandom.nextInt(5);
            switch (output)
            {
                case 1:
                    System.out.println("No. Please try again.");
                    break;
                case 2:
                    System.out.println("Wrong. Try once more.");
                    break;
                case 3:
                    System.out.println("Don’t give up!");
                    break;
                case 4:
                    System.out.println("No. Keep trying.");
                    break;
            }
            userAnswer = scnr.nextDouble();
        }
        counter++;

        if (counter == 10)
            return;

        if(userAnswer == (num1 + num2))
        {
            rightCounter++;
        }

        Add(counter,rightCounter,diffLvl,opLvl);
    }

    public static void main(String[] args)
    {
        Scanner scnr = new Scanner(System.in);
        int counter = 0;
        int rightCounter = 0;
        System.out.println("Please input difficulty level");
        int diffLvl = scnr.nextInt();
        System.out.println("Please input Operation mode");
        int opLvl = scnr.nextInt();
        switch(opLvl)
        {
            case 1:
                Add(counter, rightCounter, diffLvl, opLvl);
            case 2:
                Mul(counter, rightCounter, diffLvl, opLvl);
            case 3:
                Sub(counter, rightCounter, diffLvl, opLvl);
            case 4:
                Div(counter, rightCounter, diffLvl, opLvl);
            case 5:
                mix(counter, rightCounter, diffLvl, opLvl);

        }

    }
}