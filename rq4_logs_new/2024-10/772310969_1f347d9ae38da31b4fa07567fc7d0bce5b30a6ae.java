import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


/*
n : 양의 정수
F(n) : n의 각 자리 수 a에 대해서 그 수를 (9-a)로 바꾼 것
가장 큰 자리수의 유효숫자보다 앞에 등장하는 0들은 무시한다.
9의 반전은 0, 91의 반전은 8, 124의 반전은 875, 990의 반전은 9

사랑스러움 : n * F(n)

입력 : N
출력 : 1이상 N이하인 수들의 '사랑스러움' 중 최댓값
 */

public class B11947 {
    public static long t, n, result, a, answer, lovely;
    public static long multiplier = 1L;
    public static boolean Zero = false;
    public static long max = 0L;

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        t = Long.parseLong(br.readLine());
        for (int i = 0; i < t; i++) {
            n = Long.parseLong(br.readLine());
            answer = Lovely(n);
            System.out.println(answer);
        }
    }


    //n의 반전을 구하는 메서드
    public static long Banjeon(long n) {

        result = 0L;
        Zero = false;
        multiplier = 1L;

        //각 자리 수 a 구하기
        while (n > 0) {
            a = n % 10;
            long reverseNumber = 9 - a;

            //0이면 무시
            if (reverseNumber != 0) {
                Zero = true;
            }

            if (Zero) {
                result += reverseNumber * multiplier;
                multiplier *= 10;
            }

            n = n / 10;
        }

        return result;
    }

    //사랑스러움 구하기
    public static long Lovely(long n) {
        max = 0L;

        for (long i = 1L; i <= n; i++) {
            lovely = i * Banjeon(i);
            max = Math.max(max, lovely);
        }
        return max;
    }
}