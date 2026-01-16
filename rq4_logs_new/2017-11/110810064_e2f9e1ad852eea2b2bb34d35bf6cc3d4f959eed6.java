package com.makebono.algorithms.coinchangeproblem;

import com.makebono.algorithms.coinchangeproblem.coins.NonOptimizedCoin;

/** 
 * @ClassName: DynamicCoinChange 
 * @Description: Coin change problem by Dynamic approach, guarantees optimization.
 * @author makebono
 * @date 2017年11月16日 上午11:12:51 
 *  
 */
public class DynamicCoinChange {
    private final int total;
    // Format of the table is table[amount of input][number of each kind of coins used for this amount]
    private final int[][] solutionTable;

    // There are fifteen base cases, bacause the values of coins is 1, 10 and 15.
    // Initialize the solution table for further uses.
    private DynamicCoinChange(final int total) {
        this.total = total;
        if (total < 15) {
            this.solutionTable = new int[15][3];
            for (int i = 0; i < 9; i++) {
                this.solutionTable[i][0] = i + 1;
            }
            int temp = 0;
            for (int i = 9; i < 14; i++) {
                this.solutionTable[i][0] = temp++;
                this.solutionTable[i][1] = 1;
            }
            this.solutionTable[14][2] = 1;
        } else {
            this.solutionTable = new int[total][3];
            for (int i = 0; i < 9; i++) {
                this.solutionTable[i][0] = i + 1;
            }
            int temp = 0;
            for (int i = 9; i < 14; i++) {
                this.solutionTable[i][0] = temp++;
                this.solutionTable[i][1] = 1;
            }
            this.solutionTable[14][2] = 1;
        }
    }

    private int[][] getSolutionTable() {
        return this.solutionTable;
    }

    private int coins(final int total) {
        int result = 0;
        result += this.getSolutionTable()[total - 1][0] + this.getSolutionTable()[total - 1][1]
                + this.getSolutionTable()[total - 1][2];
        return result;
    }

    private void solve() {
        // Temporary solutions for 3 different cases: choose a loonie, a 10 dollars coin or a 15 dollars coin first.
        final int[] tempSolution = new int[3];
        tempSolution[0] = Integer.MAX_VALUE;
        tempSolution[1] = Integer.MAX_VALUE;
        tempSolution[2] = Integer.MAX_VALUE;

        for (int i = 15; i < this.total; i++) {
            if (i + 1 - NonOptimizedCoin.Loonie.getValue() >= 0) {
                tempSolution[0] = 1 + coins(i + 1 - NonOptimizedCoin.Loonie.getValue());
            }
            if (i + 1 - NonOptimizedCoin.Ten.getValue() >= 0) {
                tempSolution[1] = 1 + coins(i + 1 - NonOptimizedCoin.Ten.getValue());
            }
            if (i + 1 - NonOptimizedCoin.Fifteen.getValue() >= 0) {
                tempSolution[2] = 1 + coins(i + 1 - NonOptimizedCoin.Fifteen.getValue());
            }

            // To see which solution has minimum coins required.
            int optimal = 0;
            for (int n = 1; n < 3; n++) {
                if (tempSolution[optimal] > tempSolution[n]) {
                    optimal = n;
                }
            }

            // Update the table.
            if (optimal == 0) {
                this.getSolutionTable()[i][0] += this.getSolutionTable()[i - NonOptimizedCoin.Loonie.getValue()][0];
                this.getSolutionTable()[i][1] += this.getSolutionTable()[i - NonOptimizedCoin.Loonie.getValue()][1];
                this.getSolutionTable()[i][2] += this.getSolutionTable()[i - NonOptimizedCoin.Loonie.getValue()][2];
                this.getSolutionTable()[i][optimal]++;
            } else if (optimal == 1) {
                this.getSolutionTable()[i][0] += this.getSolutionTable()[i - NonOptimizedCoin.Ten.getValue()][0];
                this.getSolutionTable()[i][1] += this.getSolutionTable()[i - NonOptimizedCoin.Ten.getValue()][1];
                this.getSolutionTable()[i][2] += this.getSolutionTable()[i - NonOptimizedCoin.Ten.getValue()][2];
                this.getSolutionTable()[i][optimal]++;
            } else {
                this.getSolutionTable()[i][0] += this.getSolutionTable()[i - NonOptimizedCoin.Fifteen.getValue()][0];
                this.getSolutionTable()[i][1] += this.getSolutionTable()[i - NonOptimizedCoin.Fifteen.getValue()][1];
                this.getSolutionTable()[i][2] += this.getSolutionTable()[i - NonOptimizedCoin.Fifteen.getValue()][2];
                this.getSolutionTable()[i][optimal]++;
            }
        }
    }

    public static void change(final int total) {
        final DynamicCoinChange temp = new DynamicCoinChange(total);
        temp.solve();
        final int amount = temp.total;

        System.out.printf("Your total change is: %d\nDynamic approach:\n    Your change would be:\n        ", amount);

        for (int i = 2; i >= 0; i--) {
            if (temp.getSolutionTable()[total - 1][i] != 0) {
                System.out.printf(NonOptimizedCoin.values()[i].name() + ": %d\n        ",
                        temp.getSolutionTable()[total - 1][i]);
            }
        }
        System.out.printf("\nNumber of coins is: %d\n\n", temp.coins(total));
    }
}