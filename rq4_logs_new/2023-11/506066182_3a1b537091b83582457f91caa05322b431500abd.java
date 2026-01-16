import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.stream.Collectors;

public class QueueEqualizer {
    public int solution(int[] rawQueue1, int[] rawQueue2) {
        long sumQueue1 = reduce(rawQueue1);
        long sumQueue2 = reduce(rawQueue2);
        Queue<Integer> queue1 = toQueue(rawQueue1);
        Queue<Integer> queue2 = toQueue(rawQueue2);

        int queue1ElementsCount = queue1.size();
        for (
            int count = 0;
            count < queue1ElementsCount * 3;
            count += 1
        ) {
            if (sumQueue1 == sumQueue2) {
                return count;
            }

            if (sumQueue1 > sumQueue2) {
                int popped = queue1.poll();
                sumQueue1 -= popped;
                queue2.add(popped);
                sumQueue2 += popped;
            } else {
                int popped = queue2.poll();
                sumQueue2 -= popped;
                queue1.add(popped);
                sumQueue1 += popped;
            }
        }

        return -1;
    }

    private long reduce(int[] rawQueue) {
        return Arrays.stream(rawQueue)
            .mapToLong(element -> element)
            .sum();
    }

    private Queue<Integer> toQueue(int[] rawQueue) {
        return Arrays.stream(rawQueue)
            .boxed()
            .collect(Collectors.toCollection(LinkedList::new));
    }
}