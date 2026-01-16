package Models;

import java.util.ArrayList;

/**
 * Created by garci on 1/22/2017.
 */

public class CalculateTurns {
    ArrayList<Character> turnOrder = new ArrayList<>();
    int initiative1;
    int initiative2;

    public void switchCharacters(ArrayList<Character> characters, int index1, int index2){
        Character temp = characters.get(index1);
        characters.set(index1,characters.get(index2));
        characters.set(index2, temp);
    }//End of switchCharacters
    
    public void determineTurnOrder(ArrayList<Character> characters){
        for(int i = 0; i < characters.size()-1; i++){

            initiative1 = characters.get(i).getDex();
            initiative2 = characters.get(i++).getDex();
            if(initiative1 < initiative2){
                switchCharacters(characters, i, i++);
            }
        }
    }//End of determineTurnOrder
}