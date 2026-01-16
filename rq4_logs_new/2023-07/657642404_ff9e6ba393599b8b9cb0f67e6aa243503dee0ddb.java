import java.util.Arrays;

public class Prob_119 {
    public int[] plusOne(int[] digits) {
        int[] copy = new int[digits.length];
        Arrays.fill(copy,9);
        if (Arrays.equals(digits, copy)){
            copy = Arrays.copyOf(copy,digits.length+1);
            Arrays.fill(copy,0);
            copy[0]=1;
            return copy;
        }
        int c=1;
        for (int i = digits.length-1; i>=0; i--){
            if ((digits[i] += c) ==10){
                digits[i]=0;
                c=1;
            }
            else{
                break;
            }
        }
        return digits;
    }
}