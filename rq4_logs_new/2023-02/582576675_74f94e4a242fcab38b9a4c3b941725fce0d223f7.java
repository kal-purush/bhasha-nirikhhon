import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.StringTokenizer;

/**
 * BOJ 1759: 암호 만들기
 * - 메모리: 14676 kb
 * - 시간: 128 ms
 * 
 * [문제]
 * - 서로 다른 L개의 알파벳 소문자들로 구성되며 최소 한 개의 모음(a, e, i, o, u)과 최소 두 개의 자음으로 구성
 * - 알파벳은 암호에서 증가하는 순서로 배열
 * - 가능성 있는 암호들을 모두 구하여라
 * 
 * [풀이]
 * 1. 배열을 입력 받은 뒤, 알파벳을 사전순으로 정렬한 후
 * 2. 모든 경우의 수를 구해 그 중 최소 한 개의 모음과 최소 두 개의 자음으로 구성된 것을 출력
 */


public class BOJ_1759 {

    static int L, C; // 암호 길이, 사용될 문자 종류
    static char[] letters, ans; // 주어진 문자, 가능한 암호
    static StringBuilder sb; // 출력용 배열

    public static void main(String[] args) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine(), " ");

        L = Integer.parseInt(st.nextToken());
        C = Integer.parseInt(st.nextToken());

        letters = new char[C];
        st = new StringTokenizer(br.readLine(), " ");
        for (int l = 0; l < C; ++l) {
            letters[l] = st.nextToken().charAt(0);
        }
        Arrays.sort(letters); // 알파벳 증가순으로 정렬

        ans = new char[L];
        sb = new StringBuilder();

        getPassword(0,0,0,0);
        System.out.println(sb.toString());
    }

    static void getPassword(int start, int depth, int vowelCount, int consonantCount) {
        if (depth == L) {
            if (vowelCount >= 1 && consonantCount >= 2) { // 가능한 모든 경우의 수에 대해 조건 맞는지 확인
                sb.append(String.valueOf(ans)).append("\n");
            }
            return;
        }

        for (int i = start; i < C; ++i) { // 사전순으로 겹치지 않는 암호 구하기 => 조합!
            ans[depth] = letters[i];
            if (isVowel(letters[i])) {
                getPassword(i + 1, depth + 1, vowelCount + 1, consonantCount);
            } else {
                getPassword(i + 1, depth + 1, vowelCount, consonantCount + 1);
            }
        }
    }

    static boolean isVowel(char c) {
        return c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u';
    }
}