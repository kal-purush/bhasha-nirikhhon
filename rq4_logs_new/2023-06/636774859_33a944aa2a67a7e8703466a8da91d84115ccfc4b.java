import java.util.Arrays;
/**
     1502. Can Make Arithmetic Progression From Sequence
     @author Brisa Castillo
     @version 6 June 2023 15:02 BOT
     */
public class CanMakeArithmeticProgression {
    public boolean canMakeArithmeticProgression(int[] arr) {
        Arrays.sort(arr);
        int diff = arr[1]-arr[0];
        //System.out.println(diff);
        for(int i=1; i<arr.length-1; i++){
            if(arr[i+1]-arr[i] != diff)
                return false;
            //System.out.println(arr[i]);
        }
        return true;
    }
}