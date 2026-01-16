//https://leetcode.com/problems/perfect-number/description/
package algorithms.easy;

public class PerfectNumber {
    public static void main(String[] args) {
        PerfectNumber perfect = new PerfectNumber();
        System.out.println("Output:\t" + perfect.checkPerfectNumber(28));
        System.out.println("Output:\t" + perfect.checkPerfectNumber(7));
    }

    public boolean checkPerfectNumber(int num) {
        if (num == 1) return false;

        int sum = 1;
        for (int i = 2; i * i <= num; i++) {
            if (num % i == 0) {
                sum += i;
                if (i != num / i) sum += num / i;
            }
        }

        return sum == num;
    }
}