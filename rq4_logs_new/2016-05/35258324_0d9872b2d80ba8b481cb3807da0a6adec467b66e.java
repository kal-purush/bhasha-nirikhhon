package code.leetcode;

public class LongestCommonSubsequence {

	public static void main(String[] args) {
		LongestCommonSubsequence lcs = new LongestCommonSubsequence();
		System.out.println(lcs.lcs("ABCDGH", "AEDFHHR"));
	}
	
	public int lcs(String str1, String str2){
		int[][] matrix = new int[str1.length()][str2.length()];
		int diagonal,left,top = 0;
		for(int i = 0; i < str1.length(); i++){
			for(int j = 0; j < str2.length(); j++){
				if(str1.charAt(i) == str2.charAt(j)){
					diagonal = (i > 0 && j > 0)? matrix[i-1][j-1] : 0;
					matrix[i][j] = diagonal + 1;
				}else{
					left = (i > 0)? matrix[i-1][j] : 0;
					top = (j > 0)? matrix[i][j-1] : 0;
					matrix[i][j] = Math.max(left, top);
				}
			}
		}
		return matrix[str1.length() - 1][str2.length() - 1];
	}
}