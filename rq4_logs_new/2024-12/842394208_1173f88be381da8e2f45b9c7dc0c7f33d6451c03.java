import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class BOJ_1316 {

  public static void main(String[] args) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    int T = Integer.parseInt(br.readLine());
    int count = 0;
    for (int i = 0; i < T; i++) {
      Set<Character> already = new HashSet<>();
      char[] arr = br.readLine().toCharArray();
      char prev = arr[0];
      boolean isGroup = true;
      for (char c : arr) {
        if (!already.contains(c) || prev == c) {
          already.add(c);
          prev = c;
        } else {
          isGroup = false;
          break;
        }
      }
      if (isGroup) {
        count++;
      }
    }
    System.out.println(count);
  }
}