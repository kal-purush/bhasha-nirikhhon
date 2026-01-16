import java.util.Arrays;

public class Prob_1561 {
    public int maxCoins(int[] piles) {
        int length = piles.length;
        int j=length-2;
        int sum = 0;
        Arrays.sort(piles);
        for (int i=0; i<j; i++, j-=2){
            sum+=piles[j];
        }
        return sum;
    }
}