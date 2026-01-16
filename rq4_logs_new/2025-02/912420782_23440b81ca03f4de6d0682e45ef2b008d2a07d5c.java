import java.util.*;

public class sum_of_queue {


    class Solution {
        public int solution(int[] queue1, int[] queue2) {
            Queue<Integer> q1 = new LinkedList<>();
            Queue<Integer> q2 = new LinkedList<>();
            long sum1 = 0, sum2 = 0;

            for (int num : queue1) {
                q1.add(num);
                sum1 += num;
            }
            for (int num : queue2) {
                q2.add(num);
                sum2 += num;
            }

            int maxOperations = (queue1.length + queue2.length) * 2;
            int count = 0;

            while (count < maxOperations) {
                if (sum1 == sum2) {
                    return count;
                }

                if (sum1 > sum2) {
                    int num = q1.poll();
                    q2.add(num);
                    sum1 -= num;
                    sum2 += num;
                } else {
                    int num = q2.poll();
                    q1.add(num);
                    sum2 -= num;
                    sum1 += num;
                }

                count++;
            }

            return -1;
        }
    }

}