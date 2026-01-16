package main.week4.BOJ20291;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.TreeMap;

/**
 * 파일정리
 * 정렬
 * @author hazel
 */
public class BOJ20291 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine());
        TreeMap<String, Integer> map = new TreeMap<>(); //키 오름차순으로 정렬된 map

    /*
       split 에서 \\를 써야하는 이유는 split의 인자는 정규표현식이다.
       정규표현식의 .는 무작위의 한 글자를 의미하므로 사용했을때 모든 문자 기준으로 나누기 때문에 배열에 남는게 없다. 이와 같은 예로 는 |,*, & 등이 있다.
       따라서 앞에 이스케이프 문자(특수문자입력을 위해 입력하는 문자) \를 써야하는데 String 안의 이스케이프 문자 \를 사용하려면 \를 사용해야한다. 따라서 \\를 써서 .으로 문자열을 나눈다.
     */
        for (int i = 0; i < n; i++) {
            String[] strArr = br.readLine().split("\\.");
            String extension = strArr[1];
            map.put(extension, map.getOrDefault(extension, 0) + 1);
        }

        //출력
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    }

}