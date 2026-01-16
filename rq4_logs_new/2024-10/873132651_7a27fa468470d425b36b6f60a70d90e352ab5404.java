package cia2;

import java.lang.Math;

public class AlphaBetaPruning {
    static int minimax(int depth, int nodeIndex, boolean isMaximizingPlayer, int[] values, int alpha, int beta, int maxDepth) {
        // Terminal node (leaf nodes)
        if (depth == maxDepth) {
            System.out.println("Leaf node reached at depth " + depth + ", returning value: " + values[nodeIndex]);
            return values[nodeIndex];
        }

        if (isMaximizingPlayer) {
            int best = Integer.MIN_VALUE;
            System.out.println("Maximizer at depth " + depth);

            // Maximizer's choice (MAX player)
            for (int i = 0; i < 2; i++) {
                int value = minimax(depth + 1, nodeIndex * 2 + i, false, values, alpha, beta, maxDepth);
                System.out.println("Maximizer at depth " + depth + ", comparing value: " + value + " with best: " + best);
                best = Math.max(best, value);
                alpha = Math.max(alpha, best);

                // Alpha Beta Pruning
                if (beta <= alpha) {
                    System.out.println("Pruning at depth " + depth + " as beta <= alpha");
                    break;
                }
            }
            System.out.println("Maximizer at depth " + depth + ", selected best: " + best);
            return best;
        } else {
            int best = Integer.MAX_VALUE;
            System.out.println("Minimizer at depth " + depth);

            // Minimizer's choice (MIN player)
            for (int i = 0; i < 2; i++) {
                int value = minimax(depth + 1, nodeIndex * 2 + i, true, values, alpha, beta, maxDepth);
                System.out.println("Minimizer at depth " + depth + ", comparing value: " + value + " with best: " + best);
                best = Math.min(best, value);
                beta = Math.min(beta, best);

                // Alpha Beta Pruning
                if (beta <= alpha) {
                    System.out.println("Pruning at depth " + depth + " as beta <= alpha");
                    break;
                }
            }
            System.out.println("Minimizer at depth " + depth + ", selected best: " + best);
            return best;
        }
    }

    public static void main(String[] args) {
        int[] values = {3, 5, 6, 9, 1, 2, 0, -1};
        int maxDepth = (int) (Math.log(values.length) / Math.log(2));
        int result = minimax(0, 0, true, values, Integer.MIN_VALUE, Integer.MAX_VALUE, maxDepth);
        System.out.println("The optimal value is: " + result);
    }
}