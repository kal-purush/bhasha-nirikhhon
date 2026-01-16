package rish.leets.grind.medium;

import java.util.Arrays;
import java.util.Stack;

/*
 * Daily Challenge!
 * 
 * Problem #: 735
 * Problem link: https://leetcode.com/problems/asteroid-collision/
 * 
 * Date Attempted: 20/07/2023
 * 
 * @author Rishabh Soni
 *
 */
public class AsteroidCollisions {

    /*
     * Basic approach : Using stack
     * 
     * Push right moving or non-colliding elements to stack. For left moving
     * element, pop stack and push result.
     * 
     */
    public static int[] asteroidCollision(int[] asteroids) {

        Stack<Integer> stack = new Stack<>();

        for (int x : asteroids) {

            if (x > 0 || stack.isEmpty() || stack.peek() < 0) {
                stack.push(x);
                continue;
            }

            boolean undefeated = true;
            while (!stack.isEmpty() && stack.peek() > 0) {

                int a = stack.peek();
                int b = Math.abs(x);

                undefeated = a < b;

                if (a > b) { // new asteroid is defeated
                    break;
                }

                // Other asteroid is defeated
                stack.pop();
                if (a == b) {
                    break;
                }
            }

            if (undefeated) {
                stack.push(x);
            }
        }

        return stack.stream().mapToInt(Integer::intValue).toArray();
    }

    /*
     * Optimized approach: In-place modification instead of using stack
     * 
     * Convert the previous stack logic into this by using a pointer to track
     * previous asteroid.
     * 
     */
    public static int[] asteroidCollision2(int[] asteroids) {

        int j = 0;

        for (int x : asteroids) {

            while (x < 0 && j > 0 && asteroids[j - 1] > 0 && asteroids[j - 1] < Math.abs(x)) {
                j--;
            }

            if (j == 0 || x > 0 || asteroids[j - 1] < 0) {
                asteroids[j++] = x;
                continue;
            } else if (asteroids[j - 1] == Math.abs(x)) {
                j--;
            }

        }

        int[] ans = new int[j];
        System.arraycopy(asteroids, 0, ans, 0, j);
        return ans;
    }

    public static void main(String[] args) {
        int[] arr = { 5, 10, -5 };
        System.out.println(Arrays.toString(asteroidCollision2(arr)));
    }

}