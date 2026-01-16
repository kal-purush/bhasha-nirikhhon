package com.ccsi.leetcode5;

import java.util.*;

public class LC567PermutationInString {
    public static void main(String[] args) {
        String s1="ab";
        String s2="eidboaoo";
        System.out.println(checkInclusion1(s1,s2));
    }
    //method1 hashMap
    public static boolean checkInclusion(String s1,String s2){
        if(s1.length()>s2.length())return false;
        Map<Character,Integer> map=new HashMap<>();
        for(char c:s1.toCharArray()){
            map.put(c,map.getOrDefault(c,0)+1);
        }

        int start=0;
        while(start+s1.length()<s2.length()){
            Map<Character,Integer> temp=new HashMap<>(map);
            String str=s2.substring(start,start+s1.length());
            for(char c:str.toCharArray()){
                if(!temp.containsKey(c))break;
                temp.put(c,temp.get(c)-1);
                if(temp.get(c)==0)temp.remove(c);
            }
            if(temp.isEmpty())return true;
            start+=1;
        }
        return false;
    }
    //method2 因为都是小写字母，那么用数组更简洁,滑动窗的应用。比前面的应该更快一些
    public static boolean checkInclusion1(String s1,String s2){
        int len1=s1.length();
        int len2=s2.length();
        if(len1>len2)return false; //s1的长度要是小于s2的长度，显然不可能
        int[] count=new int[26]; //value就是出现的次数
        for (int i = 0; i < len1; i++) {
            count[s1.charAt(i)-'a']++; //s1中出现的字符，频率++
            count[s2.charAt(i)-'a']--; //s2中出现的字符，频率--
        }
        if(allCountZero(count))return true; //如果count里面所有的都是0，那么这段字符是可以的
        for (int i = len1; i < len2; i++) { //否则就从len1开始看s2
            count[s2.charAt(i)-'a']--; //每进一个字符，该字符频率次数--
            count[s2.charAt(i-len1)-'a']++; //同时，因为要保证这个移动窗口大小是一致的，那么必定要吐出最前面那个字符，其频率要++；
            if(allCountZero(count))return true;//一旦发现窗口内，count里面的所有的都是0，那么找到了，否则就一直找下去
        }
        return false; //一直到最后一个字符也每返回true，那么就是找不到，返回false。
    }
    private static boolean allCountZero(int[] count){
        for (int i = 0; i < 26; i++) {
            if(count[i]!=0)return false;
        }
        return true;
    }
}