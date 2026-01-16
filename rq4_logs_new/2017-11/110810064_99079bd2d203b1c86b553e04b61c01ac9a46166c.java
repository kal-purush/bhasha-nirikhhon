/* Notes: 
 * 1. It seems like bigDecimal is way faster than long, and more accurate. As for fib(40), long takes 2.8s while bigDecimal takes like 1.8s
 * 2. Trivial DP has exponential complexity, it's within O(2^n)
 * 3. With a table recording calculated sub cases can dramatically boost efficiency. For this particular case, it's O(n) with O(n) space requirement for the table. 
 * 4. Akebono kawaii.
 */
package com.makebono.algorithms.dynamicprogramming.fibonacci;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Scanner;

/** 
 * @ClassName: DPFibonacci 
 * @Description: Simple Fibonacci number calculator using DP with trivial calculation and calculation with a table to record sub cases. 
 * @author makebono
 * @date 2017年11月3日 上午10:10:43 
 *  
 */
public class DPFibonacci {
    private static ArrayList<BigDecimal> cases;

    static {
        cases = new ArrayList<BigDecimal>();
        cases.add(new BigDecimal(0));
        cases.add(new BigDecimal(1));
    }

    public static int baseCase(final int input) {
        return input;
    }

    public static BigDecimal trivialImplement(final int input) {
        if (input == 1 || input == 0) {
            return new BigDecimal(baseCase(input));
        } else {
            return trivialImplement(input - 1).add(trivialImplement(input - 2));
        }
    }

    public static BigDecimal tableAidedImplement(final int input) {
        if (input < cases.size()) {
            return cases.get(input);
        } else {
            final BigDecimal result = tableAidedImplement(input - 1).add(tableAidedImplement(input - 2));
            cases.add(result);
            return cases.get(input);
        }
    }

    public static void main(final String[] args) {
        System.out.print("Give an input: ");
        final Scanner s = new Scanner(System.in);
        final int input = s.nextInt();
        s.close();

        double t = System.currentTimeMillis();
        System.out.printf("Trivial implement.\nThe answer for trivialImplement(%d) is:", input);
        System.out.println(DPFibonacci.trivialImplement(input));
        t = System.currentTimeMillis() - t;
        System.out.printf("Trivial implement costs: %fseconds\n\n", t / 1000);

        t = System.currentTimeMillis();
        System.out.printf("Table aided implement.\nThe answer for tableAidedImplement(%d) is:", input);
        System.out.println(DPFibonacci.tableAidedImplement(input));
        t = System.currentTimeMillis() - t;
        System.out.printf("Table aided implement Costs: %fseconds", t / 1000);
    }
}