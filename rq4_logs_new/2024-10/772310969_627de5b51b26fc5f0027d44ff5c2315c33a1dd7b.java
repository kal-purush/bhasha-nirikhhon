import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.StringTokenizer;

public class B2860 {

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in)); // 선언

        //==========1. 입력한 숫자를 받아와 정수와 소수로 나눠준다.==========
        StringTokenizer st = new StringTokenizer(br.readLine(), ".");
        String integerNum = st.nextToken(); //정수
        String decimalNum = st.nextToken(); //소수

        //==========1-1. 끝자리에 0이 들어가있다면 제거해준다.(ex. 4.50 -> 4.5)==========
        int zeroCheck = 0;
        String decimalNum2 = ""; //끝자리 0이 제거된 소수

        for (int i = decimalNum.length() - 1; i >= 0; i--) { //뒤에서부터 쌓기 처음이 0이면 안넣음
            if (decimalNum.charAt(i) != '0') { //마지막이 0이 아닐시 쌓기 시작
                zeroCheck = 1;
            }
            if (zeroCheck == 1) { //오른쪽부터 하나씩 쌓아줌
                decimalNum2 = decimalNum.charAt(i) + decimalNum2;
            }
        }

        //==========2. 분자/분모로 변환한다.==========
        String numerator = integerNum + decimalNum2; //분자
        long numerator2 = Long.parseLong(numerator); //숫자로 변환한 분자(int 길이를 넘어가서 long으로 선언)
        int denominator2 = 1; //분모
        for (int i = 0; i < decimalNum2.length(); i++) { //분모 자리수만큼 곱해주기
            denominator2 = denominator2 * 10;
        }

        //==========3. 분자와 분모의 최대공약수를 구한다.==========
        long big = 0; //큰수
        long small = 0; //작은수

        if (numerator2 > denominator2) { //큰수 구하기
            big = numerator2;
            small = denominator2;
        } else {
            big = denominator2;
            small = numerator2;
        }

        int quotient, remainder, greatest; //목,나머지,최대공약수
        //최대공약수 구하기
        while (true) {
            quotient = (int) (big / small);
            remainder = (int) (big - quotient * small);
            if (remainder == 0) {
                greatest = (int) small;
                break;
            } else {
                big = small;
                small = remainder;
            }
        }
        //최대공약수로 나눠준다.
        long numerator3 = numerator2 / greatest; //분자
        long denominator3 = denominator2 / greatest; //분모


        //==========4. 출력 값을 계산한다.==========
        long numerator4 = numerator3;
        //1~5의 숫자 (denominator3[분모])개로 (numerator3[분자])를 만들어주면됨..
        int minusNum = 5; //1~5중 가장 큰 수인 5부터 뺀다.
        int[] NumCount = new int[5];
        for (int i = 0; i < denominator3; i++) { //분모만큼 반복해야한다.

            //분자에서 minusNum을 빼고 남은 수가 분모보다 작으면 안되기때문에 조건을 걸어준다.
            if ((denominator3 - i - 1) <= (numerator4 - minusNum)) { //(분모-현재순번-1)<=(분자-빼려는 값)
                numerator4 = numerator4 - minusNum;
                NumCount[minusNum - 1] = NumCount[minusNum - 1] + 1; //숫자별로 얼만큼 사용되었는지 적립한다.(numCount[0]은 1을 사용한 횟수)
            } else {
                minusNum = minusNum - 1; //더 낮은수로 시도
                i = i - 1; //이번 순번 무효
            }
        }

        // 결과 출력
        System.out.println(NumCount[0] + " " + NumCount[1] + " " + NumCount[2] + " " + NumCount[3] + " " + NumCount[4]);
        br.close();
    }
}