package implementation.p11866;

import implementation.Implementation;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class Main {

    public static void main(String[] args) throws IOException {

        System.setIn(new FileInputStream(Implementation.PATH + "/p11866/data/1.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());

        int peopleNum = Integer.parseInt(st.nextToken());
        int interval = Integer.parseInt(st.nextToken());

        ArrayList<Integer> seq  = new ArrayList<>(peopleNum); // < 초기화 good
        for (int i = 0; i < peopleNum; i++)
            seq.add(i + 1);

        int[] result = new int[peopleNum];
        int resultIndex = 0, seqIndex = 0, intervalIndex = 0;
        while (seq.size() > 1) {

            if (seqIndex == seq.size()) {
                seqIndex = 0;
                continue;
            }

            if (intervalIndex == interval - 1) {
                intervalIndex = 0;
                result[resultIndex] = seq.get(seqIndex);
                seq.remove(seqIndex);
                ++resultIndex;
                continue;
            }

            ++seqIndex;
            ++intervalIndex;
        }
        result[resultIndex] = seq.get(0);

        // 출력 부분
        System.out.printf("<");
        for (int i = 0; i < result.length; i++) {
            System.out.printf("%d", result[i]);
            if (i < result.length - 1)
                System.out.printf(", ");
        }
        System.out.printf(">");
    }
}