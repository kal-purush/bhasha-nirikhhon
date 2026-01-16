import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class BOJ_2675 {

  public static void main(String[] args) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));

    String input;
    while ((input = br.readLine()) != null && !input.isEmpty()) {
      String[] arr = input.split(" ");
      if (arr.length < 2) {
        continue;
      }
      int repeat = Integer.parseInt(arr[0]);
      String str = arr[1];

      for (char c : str.toCharArray()) {
        bw.write(String.valueOf(c).repeat(repeat));
      }
      bw.newLine();

    }
    bw.flush();
  }
}