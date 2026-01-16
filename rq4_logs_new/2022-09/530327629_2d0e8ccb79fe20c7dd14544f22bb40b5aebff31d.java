import java.util.Arrays;

public class RowColMatrix {
    public static void main(String[] args) {
        int[][] rowCol={
           {1,4,7,11,15},
           {2,5,8,12,19},
           {3,6,9,16,22},
           {10,13,14,17,24},
           {18,21,23,26,30}
        };
        int[] ans =Search(rowCol,5);
        System.out.println(Arrays.toString(ans));
    }

    private static int[] Search(int[][] matrix, int target) {
       int r=0;
       int c=matrix.length-1;
         while(r<matrix.length && c>=0){
            if(target==matrix[r][c]){
                return new int[]{r,c};
            }
            if(target>matrix[r][c]){
                r++;
            }else{
                c--;
            }
         }
         return new int[]{-1,-1};
            }
            
         }