package BaekJoon.String;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner;

/*
    2993 세 부분
    문자열
 */
public class Solution2 {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        String s = sc.next();
        ArrayList<String> arrayList = new ArrayList<>();
        for(int k = 0; k < s.length()-2; k++){
            for(int i = k+1; i < s.length()-1; i++){
                for(int j = i+1; j < s.length(); j++){
                    if(s.length() == s.substring(k, i).length() + s.substring(i, j).length() + s.substring(j).length()){
                        String tmp = "";
                        StringBuilder a = new StringBuilder();
                        a.append(s.substring(k, i));
                        StringBuilder b = new StringBuilder();
                        b.append(s.substring(i, j));
                        StringBuilder c = new StringBuilder();
                        c.append(s.substring(j));
                        arrayList.add(a.reverse().toString() + b.reverse().toString() + c.reverse().toString());
                    }
                }
            }
        }
        Collections.sort(arrayList);
        System.out.println(arrayList.get(0));
    }
}