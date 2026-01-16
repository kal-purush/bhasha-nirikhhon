import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayDeque;
import java.util.Deque;

public class BOJ_17608 {

  public static void main(String[] args) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    int N = Integer.parseInt(br.readLine());
    int max = 0;

    Deque<Integer> stack = new ArrayDeque<>();
    for (int i = 0; i < N; i++) {
      stack.push(Integer.parseInt(br.readLine()));
    }

    int count = 0;
    StringBuilder sb = new StringBuilder();
    while (!stack.isEmpty()) {
      int current = stack.pop();
      if (max < current) {
        count++;
        max = current;
      }
    }
    System.out.println(count);
  }

}