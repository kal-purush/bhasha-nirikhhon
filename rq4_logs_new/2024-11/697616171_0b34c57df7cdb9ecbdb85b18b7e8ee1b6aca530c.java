import java.util.Scanner;

public class Search2DMatrix {
    static boolean searchMatrix(int[][] mat,int target){
        int i=0,j=3;
        while(i<3 && j>=0){
            if(mat[i][j]>target){
                j--;
            }
            else if(mat[i][j]<target){
                i++;
            }
            else{
                return true;
            }
        }
        return false;
    }
    public static void main(String[] args) {
        Scanner sc=new Scanner(System.in);
        int[][] mat=new int[3][4];
        for (int i = 0; i < mat.length; i++) {
            for (int j = 0; j < mat[i].length; j++) {
                mat[i][j]=sc.nextInt();
            }
        }
        int target=sc.nextInt();
        System.out.println(searchMatrix(mat, target));


        sc.close();
    }
}