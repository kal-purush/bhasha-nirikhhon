import java.io.*;

public class BOJ_2231 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        Integer N = Integer.parseInt(br.readLine());
        int min = 0;

        for (int i = 1; i <= N; i++) {
            String strI = String.valueOf(i);
            int sum = i;
            for (int j = 0; j < strI.length(); j++) {
                sum += strI.charAt(j) - '0';
            }

            if (sum == N) {
                min = i;
                break;
            }
        }

        System.out.println(min);
    }
}