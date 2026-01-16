import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayDeque;
import java.util.Deque;

public class BOJ_9012 {

  public static void main(String[] args) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    StringBuilder sb = new StringBuilder();

    int n = Integer.parseInt(br.readLine());

    for (int i = 0; i < n; i++) {
      char[] input = br.readLine().toCharArray();
      Deque<Character> stack = new ArrayDeque<>();
      String isVPS = "YES";
      for (char c : input) {
        if (c == '(') {
          stack.push(c);
        } else {
          if (stack.isEmpty()) {
            isVPS = "NO";
            break;
          }
          stack.pop();
        }
      }
      if (!stack.isEmpty()) {
        isVPS = "NO";
      }
      sb.append(isVPS).append("\n");
    }
    System.out.println(sb);
  }
}