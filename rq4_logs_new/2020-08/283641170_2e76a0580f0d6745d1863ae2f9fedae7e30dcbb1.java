class GenerateParanthesis {
    public List<String> generateParenthesis(int n) {
        if(n==0)
            return new ArrayList<>();
        List<String> ans = new ArrayList<>();
        generateParanthesis(n,ans,1,"(");
        return ans;
    }
    void generateParanthesis(int n, List<String> ans, int sum, String curr){
        if(curr.length() == 2*n){
            if(sum==0)
                ans.add(curr);
            return;
        }
        if(sum<0)
            return;
        generateParanthesis(n,ans,sum-1,curr+")");
        generateParanthesis(n,ans,sum+1,curr+"(");

    }
}

//TIME COMPLEXITY: O(2^n). it is less than 2^n at any given character 2 strings are generated if valid.
