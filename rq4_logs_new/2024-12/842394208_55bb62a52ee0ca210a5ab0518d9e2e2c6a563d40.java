import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.StringTokenizer;

public class BOJ_12605 {

  public static void main(String[] args) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    int n = Integer.parseInt(br.readLine());
    for (int i = 1; i <= n; i++) {
      Deque<String> stack = new ArrayDeque<>();
      StringBuilder sb = new StringBuilder("Case #" + i + ": ");
      StringTokenizer st = new StringTokenizer(br.readLine());
      while (st.hasMoreTokens()) {
        stack.push(st.nextToken());
      }
      while (!stack.isEmpty()) {
        sb.append(stack.pop()).append(" ");
      }
      System.out.println(sb);
    }
  }
}