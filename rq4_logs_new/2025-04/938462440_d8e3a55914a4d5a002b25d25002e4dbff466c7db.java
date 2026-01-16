package dsaQuetions.googleMedium;

public class PascalTriangle {

    //  Time complexity: O(r) -> r is number of columns
    //  Space complexity:
    //          Extra: O(1)
    //          Algo: O(1)
    //  Pattern: NCR
    private static int getNCR(int n, int r) {
        int res = 1;
        for (int i = 0; i < r; i++) {
            res *= (n-i);
            res /= (i + 1);
        }
        return res;
    }


    public static int getElementInPascalTriangleAt(int atRow, int atCol) {
        int result = getNCR(atRow - 1, atCol - 1);
        return result;
    }
}