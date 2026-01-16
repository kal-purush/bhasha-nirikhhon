import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


/*
제곱의 재귀.

입력 : 높이 H
출력 : H 높이의 2타워 값을 3으로 나눈 나머지를 출력.
 */
public class B10407 {

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        long h = Long.parseLong(br.readLine());
        int answer = Answer(h);

        System.out.println(answer);


    }

    public static long Tower(long h){

        if(h == 1) {
            return 2;
        }
        long height = Tower(h-1);

        return height * height; //1층 2 2층: 1층 * 1층 3층 : 2층 * 2층 4층 : 3층 * 3층
    }

    public static int Answer(long h){
        return (int) Tower(h)%3;
    }
}