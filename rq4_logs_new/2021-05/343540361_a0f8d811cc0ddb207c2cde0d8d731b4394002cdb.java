package problem630;

import java.util.Arrays;
import java.util.PriorityQueue;

public final class CourseSchedule3 {

    public int scheduleCourse(final int[][] courses) {
        Arrays.sort(courses, (a1, a2) -> a1[1] - a2[1]);

        final PriorityQueue<Integer> durationQ = new PriorityQueue<>((a, b) -> b - a);
        int currentTime = 0;

        for (final int[] course : courses) {
            final int dur = course[0];
            final int deadline = course[1];

            if (currentTime + dur <= deadline) {
                currentTime += dur;
                durationQ.offer(dur);
            } else if (!durationQ.isEmpty()) {
                final int longest = durationQ.peek();
                if (longest > dur && currentTime + dur - longest <= deadline) {
                    currentTime += dur - durationQ.remove();
                    durationQ.offer(dur);
                }
            }
        }

        return durationQ.size();
    }

}