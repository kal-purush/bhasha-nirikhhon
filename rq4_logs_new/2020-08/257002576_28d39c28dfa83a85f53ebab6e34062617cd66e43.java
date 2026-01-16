import java.util.*;
class Solution {
    public int solution(int[] citations) {
        int index = 0;
        int n = citations.length;
        ArrayList<Integer> al = new ArrayList<Integer>();
        
        for(int i = 0; i < n; i++){
            al.add(citations[i]);
        }
        
        Collections.sort(al);
        
        //모든 논문의 인용 횟수가 논문의 갯수보다 많을 때
        if(al.get(0) >= n){
                return n;
        }
        
        while(index < n){
            int h = al.get(index);
            if( h > n - index){
                System.out.println(h);
                System.out.println(n - index);
                while(h > al.get(index-1) ){
                    System.out.println(h);
                    if( --h <= n - index)
                        return h;
                }
                return al.get(index-1);
            }
            index++;
        }
        return al.get(0);
    }
}