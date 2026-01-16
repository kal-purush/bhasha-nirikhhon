//https://leetcode.com/problems/teemo-attacking/
package algorithms.easy;

public class TeemoAttacking {
    public static void main(String[] args) {
        TeemoAttacking teemo = new TeemoAttacking();
        System.out.println("Output:\t" + teemo.findPoisonedDuration(new int[]{1, 4}, 2));
        System.out.println("Output:\t" + teemo.findPoisonedDuration(new int[]{1, 2}, 2));
    }

    public int findPoisonedDuration(int[] timeSeries, int duration) {
        int n = timeSeries.length;
        if (n == 0)
            return 0;
        int answer = duration;
        for (int i = 1; i < n; i++) {
            int diff = timeSeries[i] - timeSeries[i - 1];
            answer += Math.min(diff, duration);
        }
        return answer;
    }
}