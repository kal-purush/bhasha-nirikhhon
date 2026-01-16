import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class BOJ_6603 {
    public static void main(String[] args) throws IOException {
        // 독일로또 1~49
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            String str = br.readLine();
            if (str.equals("0")) {
                break;
            }

            String[] arr = str.split(" ");

            int[] numbers = new int[arr.length - 1];
            boolean[] visited = new boolean[arr.length + 1];
            int[] lotto = new int[6];

            for (int i = 0; i < arr.length - 1; i++) {
                numbers[i] = (Integer.parseInt(arr[i + 1]));
            }

            dfs(numbers, visited, lotto, 0, 0);
            System.out.println();
        }

    }

    private static void dfs(int[] numbers, boolean[] visited, int[] lotto, int depth, int now) {
        if (depth == 6) {
            print(lotto);
            return;
        }

        for (int i = now; i < numbers.length; i++) {
            if (!visited[i]) {
                visited[i] = true;
                lotto[depth] = numbers[i];
                dfs(numbers, visited, lotto, depth + 1, i);
                visited[i] = false;
            }
        }
    }

    public static void print(int[] arr) {
        for (int i : arr) {
            System.out.print(i + " ");
        }
        System.out.println();
    }
}