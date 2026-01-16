package xsd;
import java.text.NumberFormat;
import java.util.Locale;

public class Computeclass {
    /*
     * 计算相似度
     * */
    public static double SimilarDegree(String strA, String strB){
        String newStrA = removeSign(strA);
        String newStrB = removeSign(strB);
        //用较大的字符串长度作为分母，相似子串作为分子计算出字串相似度
        int temp = Math.max(newStrA.length(), newStrB.length());
        int temp2 = longestCommonSubstring(newStrA, newStrB);
        return temp2 * 1.0 / temp;
    }
    /*
     * 将字符串的所有数据依次写成一行
     * */
    public static String removeSign(String str) {
        StringBuffer sb = new StringBuffer();
        //遍历字符串str,如果是汉字数字或字母，则追加到ab上面
        for (char item : str.toCharArray())
            if (charReg(item)){
                sb.append(item);
            }
        return sb.toString();
    }
    /*
     * 判断字符是否为汉字，数字和字母，
     * 因为对符号进行相似度比较没有实际意义，故符号不加入考虑范围。
     * */
    public static boolean charReg(char charValue) {
        return (charValue >= 0x4E00 && charValue <= 0X9FA5) || (charValue >= 'a' && charValue <= 'z')
                || (charValue >= 'A' && charValue <= 'Z')  || (charValue >= '0' && charValue <= '9');
    }


    /*
     * 求公共子串，采用动态规划算法。
     * 其不要求所求得的字符在所给的字符串中是连续的。
     *
     * */
    public static int longestCommonSubstring(String str1, String str2) {
        int length1 = (short) str1.length();
        int length2 = (short) str2.length();
        int[][] result = new int[2][length2 + 1];   //滚动数组,节省空间只依赖于前面的两个解
        for (int i = 1; i <= length1; i++) {
            for (int j = 1; j <= length2; j++) {
                if (str1.charAt(i - 1) == str2.charAt(j - 1))
                    result[i % 2][j] = (short) (result[(i - 1) % 2][j - 1] + 1);
                else
                    result[i % 2][j] = result[(i - 1) % 2][j] > result[i % 2][j - 1] ? result[(i - 1) % 2][j] : result[i % 2][j - 1];
            }
        }
        return result[length1 % 2][length2];
    }

    /*
     * 结果转换成百分比形式
     * */
    public static String similarityResult(double resule){
        return  NumberFormat.getPercentInstance(new Locale( "en ", "US ")).format(resule);
    }
}