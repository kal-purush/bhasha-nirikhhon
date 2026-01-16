import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ClimbingTheLeaderBoard {
  public static void main(String[] args) {
    ClimbingTheLeaderBoard.climbingLeaderboard(new int[]{100, 90, 90, 80, 75, 60}, new int[]{50, 65, 77, 90, 102});
  }

  private static int[] climbingLeaderboard(int[] scores, int[] alice) {
    TreeSet<Integer> treeSet = new TreeSet<>();

    for (int value : scores) {
      treeSet.add(value);
    }

    NavigableSet<Integer> navigableSet = treeSet.descendingSet();

    AtomicInteger integer = new AtomicInteger(1);

    Map<Integer, Integer> map = new HashMap<>();

    navigableSet.forEach((item) -> map.put(item, integer.getAndIncrement()));

    int len = alice.length;

    int[] aliceScores = new int[len];

    for (int i = 0; i < alice.length; i++) {
      if (alice[i] > navigableSet.first()) {
        aliceScores[i] = 1;
      } else if (alice[i] < navigableSet.last()) {
        aliceScores[i] = map.get(navigableSet.last()) + 1;
      } else {
        aliceScores[i] = map.get(navigableSet.ceiling(alice[i]));
      }
    }

    return aliceScores;
  }
}