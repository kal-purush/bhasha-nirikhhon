import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class bk_1149 {

    public static void main(String[] args) throws IOException {

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine());

        String input = "";

        int[][] arr = new int[n][3];
        for (int i = 0; i < n; i++) {
            input = br.readLine();
            arr[i][0] = (Integer.parseInt(input.split(" ")[0]));
            arr[i][1] = (Integer.parseInt(input.split(" ")[1]));
            arr[i][2] = (Integer.parseInt(input.split(" ")[2]));
        }

        for (int i = 1; i < n; i++) {
            int red = arr[i][0];
            int green = arr[i][1];
            int blue = arr[i][2];

            arr[i][0] = red + Math.min(arr[i - 1][1], arr[i - 1][2]);
            arr[i][1] = green + Math.min(arr[i - 1][0], arr[i - 1][2]);
            arr[i][2] = blue + Math.min(arr[i - 1][1], arr[i - 1][0]);

        }

        System.out.println(Math.min(Math.min(arr[n - 1][0], arr[n - 1][1]), arr[n - 1][2]));

    }

}