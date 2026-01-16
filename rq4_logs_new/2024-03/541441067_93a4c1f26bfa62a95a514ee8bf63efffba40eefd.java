class Solution {
    int uppableValue = 3, unUppableValue = 2;
    public int solution(int n, int[] tops) {
        int length = tops.length;
        int[][] result = new int[2][length]; //{오른마름모, 그외}
        if(tops[0] == 1){
            result[0][0] = 1;
            result[1][0] = uppableValue;
        }else{
            result[0][0] = 1;
            result[1][0] = unUppableValue;
        }
        for(int i=1; i<length; i++){
            result[0][i] = result[1][i-1] + result[0][i-1];
            if(tops[i]==1){
                //uppable
                result[1][i] = (result[0][i-1] * (uppableValue-1))%10007
                    +(result[1][i-1] * (uppableValue))%10007;
            }else{
                result[1][i] = (result[0][i-1] * (unUppableValue-1))%10007
                    + (result[1][i-1] * (unUppableValue))%10007;
            }
        }
        
        return (result[0][length-1] + result[1][length-1])%10007;
    }
}