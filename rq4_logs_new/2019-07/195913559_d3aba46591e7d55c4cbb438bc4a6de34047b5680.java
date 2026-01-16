import java.util.ArrayList;
import java.util.Scanner;


public class UserInputManager {


    public static ArrayList<Integer> getGridDimensions(){
        Scanner userInput = new Scanner(System.in);
        ArrayList<Integer> dimensions = new ArrayList<>();
        String[] enteredDimensions = userInput.next().split(",");
        for(String dimension : enteredDimensions){
            int dimensionAsInt = Integer.parseInt(dimension);
            dimensions.add(dimensionAsInt);
        }
        return dimensions;
    }
}