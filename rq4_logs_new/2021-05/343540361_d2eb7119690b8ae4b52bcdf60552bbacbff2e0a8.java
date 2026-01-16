package problem1354;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

public final class MultiSumArray {

    public boolean isPossible(final int[] target) {
        int sum = 0;
        final Queue<Integer> sortingQ = new PriorityQueue<>(Comparator.reverseOrder());

        for (final int n : target) {
            sum += n;
            sortingQ.add(n);
        }

        while (true) {
            int max = sortingQ.poll();
            if (max == 1)
                return true;

            sum -= max;

            if (sum < 1 || max <= sum)
                return false;

            max %= sum;
            sum += max;
            sortingQ.offer(max);
        }
    }

}