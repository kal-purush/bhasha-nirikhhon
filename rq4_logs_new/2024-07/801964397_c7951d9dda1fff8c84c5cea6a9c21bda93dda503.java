package No11399;

import java.io.*;
import java.util.*;
public class Main {
    public static void main(String[] args) throws IOException{
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine());
        int[] time = new int[n];    //각 사람이 atm을 이용하는 시간
        int[] waiting_time = new int[n];    //각 사람들의 atm 사용이 끝나는 시간
        StringTokenizer st = new StringTokenizer(br.readLine()," ");

        for (int i = 0; i < n; i++) {
            time[i] = Integer.parseInt(st.nextToken());
        }

        //오름차순으로 정렬되어있어야 누적시간이 최솟값이 되므로
        Arrays.sort(time); //오름차순 정렬

        int k=0;
        int sum=0; //k번째 사람이 atm 사용이 끝나는 시간
        int answer = 0;
        for (int t : time){
            sum+=t;
            waiting_time[k] = sum;
            answer += waiting_time[k];
            k++;
        }
        System.out.println(answer);

    }
}