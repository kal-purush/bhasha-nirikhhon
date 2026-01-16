package inexOf;

import java.util.ArrayList;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        ArrayList<Integer> list = new ArrayList<>();

        while (true) {

            int input = Integer.parseInt(scanner.nextLine());

            if (input == -1) {

                System.out.println("Search for? ");
                int inputSearch = scanner.nextInt();

            for(int i=0; i<=list.size()-1; i++){

                int number = list.get(i);

                if(inputSearch == number){
                    System.out.println(inputSearch + " is at index " + i);
                }

            }




                break;
            }


            list.add(input);
        }



        System.out.println("");

        // implement here finding the indices of a number
    }



}