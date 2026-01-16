import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.StringTokenizer;

public class bk_11399{
    static StringBuilder sb = new StringBuilder();
    static StringTokenizer st;
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine());
        int bankArr[] = new int[n];

        st = new StringTokenizer(br.readLine());
        for (int i = 0; i < bankArr.length; i++) {
            bankArr[i] = Integer.parseInt(st.nextToken());
        }

        Arrays.sort(bankArr);

        int time = 0;
        int totalTime = 0;
        for (int i = 0; i < bankArr.length; i++) {
            time += bankArr[i];
            totalTime +=time;
        }
        sb.append(totalTime);

        System.out.println(sb);

    }
}