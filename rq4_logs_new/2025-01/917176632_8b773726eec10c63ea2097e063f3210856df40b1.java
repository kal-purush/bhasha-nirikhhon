class Solution {
    public List<List<Integer>> generate(int numRows) {
        List<List<Integer>> ans = new ArrayList<>();
        for(int rowNum=0;rowNum<numRows;rowNum++){
            List<Integer> rowans = new ArrayList<>();
            rowans.add(1);
            for(int i=1;i<rowNum;i++){
                int val = ans.get(rowNum-1).get(i-1) + ans.get(rowNum-1).get(i);
                rowans.add(val);
            }

            if(rowNum > 0){
                rowans.add(1);
            }

            ans.add(rowans);
            

        }
        return ans;
    }
}