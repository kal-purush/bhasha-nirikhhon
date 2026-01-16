package my.com.app;

import java.util.*;
import java.util.PriorityQueue;
import java.util.Map.Entry;

// Find top K Frequent Elements
// Use HashMap to store the frequency of each element
// Use PriorityQueue to store the elements in descending order of frequency
// Use a custom comparator to sort the elements based on frequency


class Solution {
    public int[] topKFrequent(int[] nums, int k) {
         HashMap<Integer, Integer> hm = new HashMap();
        
        for(int i =0 ;i <nums.length; i++){
            hm.put(nums[i],hm.getOrDefault(nums[i], 0)+1);
        }
    
        PriorityQueue<Map.Entry<Integer,Integer>> entryset = new PriorityQueue(new Comparator<Map.Entry<Integer,Integer>>(){
            
            public int compare(Map.Entry<Integer,Integer> e1 , Map.Entry<Integer,Integer> e2){
                
                return  (e2.getValue() <= e1.getValue())?-1:1 ;
            }
        });
        
        for(Map.Entry<Integer,Integer> e : hm.entrySet()){
            entryset.add(e);
        }
    
        
        int[] res = new int[k];
        int i =0;
        while(i<k){
            //System.out.println(entryset.last());
            res[i] = entryset.poll().getKey();
            i++;
        }
        
    return res;
    }
}