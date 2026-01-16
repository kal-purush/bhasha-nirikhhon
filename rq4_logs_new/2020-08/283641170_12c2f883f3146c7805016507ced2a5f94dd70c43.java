//Question: return lenths of partitions such that each element is in atmost one partition.
//Intuition: Each character first occurence and last occurence should be in the same partition. Use greedy approach.
// 1. maintain the list of last occurence of each character starting from the left of the string
//2.  iterate over the string again. allot the end of label "j" to its last occurence.
//3. add the length to ans list only when the iterate i reaches the j;
//4. once i ==j, calculate the length from starting "anchor" to i and then point anchor to i+1;

//Time complexity: O(n)


class PartitonElements {
    public List<Integer> partitionLabels(String S) {
        int[] last = new int[26];
        List<Integer> ans = new ArrayList();
        //stored last occurence of the character
        for(int i = 0;i<S.length();i++){
            last[S.charAt(i)-'a'] = i;
        }
        int j = 0, anchor = 0;
        for(int i = 0;i<S.length();i++){
            j = Math.max(j,last[S.charAt(i)-'a']);
            if(i==j){
                ans.add(i-anchor+1);
                anchor = i+1;
            }
        }
        return ans;
    }
}