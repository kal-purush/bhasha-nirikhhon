package O2_DSA_intermediate.O3_02052022_intermediate_dsa_time_complexity_2;

public class MoneyChange {

    static int[] coins = {1,2,5,10,20,50, 100, 200, 500, 2000};
    private static void changeMoney(int num) {

        // base case
        if(num == 0) {
            return;
        }

        // recursive case
        for(int i = coins.length - 1; i >= 0; i--) {
            if(coins[i] <= num) {
                System.out.println("Pick: " + coins[i]);
                changeMoney(num - coins[i]);
                break;
            }
        }

    }
    // TC - O(n)
    // SC - O(n)

    public static void main(String[] args) {
        changeMoney(2001);
    }
}