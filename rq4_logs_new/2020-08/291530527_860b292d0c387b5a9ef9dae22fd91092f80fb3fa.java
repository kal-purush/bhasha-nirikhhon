//time complexity : O(nlogk) where k is the number of unique characters in the string and n is the length of the string
//space complexity: O(k) where k is the number of unique characters in a string
//executed on leetcode: yes

class Solution {
    class Pair
    {
        char key;
        int val;
        public Pair(char key,int val)
        {
            this.key = key;
            this.val=val;
        }
    }
    public String reorganizeString(String S) {
        if(S==null||S.length()==0) return "";
        PriorityQueue<Pair> pq = new PriorityQueue<Pair>((a,b)->b.val-a.val);
        StringBuilder sb = new StringBuilder();
        HashMap<Character,Integer> map = new HashMap<>();
        for(int i=0;i<S.length();++i)
        {
            map.put(S.charAt(i),map.getOrDefault(S.charAt(i),0)+1);
        }
        for(char c : map.keySet())
        {
            pq.add(new Pair(c,map.get(c)));
        }
        char last_char=' ';
        Pair lp = new Pair(' ',-1);
        while(!pq.isEmpty())
        {
            Pair curr = pq.poll();
            char c = curr.key;
            if(c!=last_char)
            {
                sb.append(c);
                if(curr.val-1>0) pq.add(new Pair(c,curr.val-1));
                last_char = c;
            }
            else
            {
                lp = curr;
                if(pq.isEmpty()) return "";
                curr = pq.poll();
                c = curr.key;
                sb.append(c);
                if(curr.val-1>0) pq.add(new Pair(c,curr.val-1));  
                pq.add(lp);
                last_char = c;
            }
        }
        return sb.toString();
        
    }
}