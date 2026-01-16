import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.stream.IntStream;

public class BOJ_1065 {

  public static void main(String[] args) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    int n = Integer.parseInt(br.readLine());
    if (n < 100) {
      System.out.println(n);
    } else if (n < 111) {
      System.out.println(99);
    } else {
      long result = IntStream.rangeClosed(111, n)
          .filter(i -> {
            int[] arr = Arrays.stream(String.valueOf(i).split(""))
                .mapToInt(Integer::valueOf)
                .toArray();
            int size = arr[arr.length - 1] - arr[arr.length - 2];
            for (int j = arr.length - 2; j > 0; j--) {
              if (size != arr[j] - arr[j - 1]) {
                return false;
              }
            }
            return true;
          })
          .count();
      System.out.println(result + 99);
    }
  }
}