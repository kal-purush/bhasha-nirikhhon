public class MinimalDistance {
    public int minDistance(String str1, String str2) {
        int[][] array = new int[str1.length()+1][str2.length()+1];
        array[0][0] = 0;
        for (int j = 1; j <= str2.length(); j++) {
            array[0][j] = array[0][j - 1] + 1;
        }
        for (int i = 1; i <= str1.length(); i++) {
            array[i][0] = array[i - 1][0] + 1;
            for (int j = 1; j <= str2.length(); j++) {
                if (str1.charAt(i-1) == str2.charAt(j-1)) {
                    array[i][j] = array[i-1][j-1];
                } else {
                    array[i][j] = Math.min(array[i-1][j],Math.min(array[i-1][j-1],array[i][j-1])) + 1;
                }
            }
        }
        return array[str1.length()][str2.length()];
    }
}