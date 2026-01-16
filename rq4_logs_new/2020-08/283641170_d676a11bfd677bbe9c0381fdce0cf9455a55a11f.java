class LetterCombination {

    private Map<Character, String> map = new HashMap<Character, String>() {{
        put('2', "abc");
        put('3', "def");
        put('4', "ghi");
        put('5', "jkl");
        put('6', "mno");
        put('7', "pqrs");
        put('8', "tuv");
        put('9', "wxyz");
    }};


    public List<String> letterCombinations(String digits) {
        if(digits.length()==0)
            return new ArrayList<>();

        List<String> ans = new ArrayList<>();
        letterCombinations(digits,0,ans,"");
        return ans;
    }

    void letterCombinations(String digits,int i,List<String> ans,String currList){
        if(i==digits.length()){
            ans.add(currList);
            return;
        }
        char[] letters = map.get(digits.charAt(i)).toCharArray();
        for(char l : letters){
            letterCombinations(digits,i+1,ans,currList+String.valueOf(l));
        }
    }
}