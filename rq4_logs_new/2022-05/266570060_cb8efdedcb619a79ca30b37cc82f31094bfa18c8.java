package array;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Solution {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//System.out.println(maxTripletSum(new int[] { 2, 3, 4, -10,15}));
		

	}
	
	private static int max_min(int[] arr) {
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        for(int i=0;i<arr.length;i++){
            if(arr[i]>max)
                max=arr[i];
            if(arr[i]<min)
                min=arr[i];    
        }
        return max+min;
    }
	
	private static int maxSubArray(final int[] arr) {
        int max = Integer.MIN_VALUE;
        int c = 0;
        for(int i=0;i<arr.length;i++){
        	c = c+arr[i];
            max = c>max?c:max;
            c=c<0?0:c;
        }
		return max;
    }
	
	
	
	
//	private static String[] uncommonFromSentences(String s1, String s2) {
//        Map<String, Integer> m=new LinkedHashMap<>();
//        s1=s1.trim();
//        s2=s2.trim();
//        String a[] = s1.split(" ");
//        String b[] = s2.split(" ");
//
//        for(String s:a)
//        	m.put(s, m.getOrDefault(s, 0)+1);
//        for(String s:b)
//        	m.put(s, m.getOrDefault(s, 0)+1);
//        String res[]=new String[];
//        for(String s:m.keySet()) {
//        	if(m.get(s)==1)
//        	
//        }
//        	
//       // Object[] r = res.toArray();
//		return null;
//        
//        
//    }
}