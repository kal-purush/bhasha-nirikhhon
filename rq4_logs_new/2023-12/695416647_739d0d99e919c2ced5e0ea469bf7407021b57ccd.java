import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BOJ_10815 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int N = Integer.parseInt(br.readLine());
        String[] numbers = br.readLine().split(" ");

        Set<String> list = Set.of(numbers);

        int M = Integer.parseInt(br.readLine());
        String[] targets = br.readLine().split(" ");

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < M; i++) {
            sb.append(list.contains(targets[i]) ? 1 : 0).append(" ");
        }

        System.out.println(sb.toString());
    }
}