package Grades;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

public class GroceryList {

    static HashMap<String, List<HashMap<String, String>>> groceryList = new HashMap<>();



        public static void main(String[] args){
        Scanner scanner = new Scanner(System.in);
        System.out.println("Would you like to create a grocery list?: [y/N]");
        String makeList = scanner.next();
        if(makeList.equalsIgnoreCase("y")){
            System.out.println("Would you like to enter a new item now?: [y/n]");
            String enterItem = scanner.next();
            if(enterItem.equalsIgnoreCase("y")){
                System.out.println("getting closer");
            }
        }
    }
}