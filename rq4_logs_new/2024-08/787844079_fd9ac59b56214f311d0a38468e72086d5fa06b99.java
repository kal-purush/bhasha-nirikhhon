//https://leetcode.com/problems/complement-of-base-10-integer/
package algorithms.easy;

public class ComplementOfBase10Integer {
    public static void main(String[] args) {
        ComplementOfBase10Integer complement = new ComplementOfBase10Integer();
        System.out.println("Output:\t" + complement.bitwiseComplement(5));
        System.out.println("Output:\t" + complement.bitwiseComplement(7));
        System.out.println("Output:\t" + complement.bitwiseComplement(10));
    }

    public int bitwiseComplement(int n) {
        if (n == 0)
            return 1;

        int answer = 0;
        int power = 1;
        while (n > 0) {
            answer += ((n % 2) ^ 1) * power;
            power *= 2;
            n /= 2;
        }
        return answer;
    }
}