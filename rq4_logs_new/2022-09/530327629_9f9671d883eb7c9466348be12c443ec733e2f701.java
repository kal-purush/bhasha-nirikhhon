//https://leetcode.com/problems/count-negative-numbers-in-a-sorted-matrix/
public class NegativeCount {
    public static void main(String[] args) {
        int[][] grid={
            {4,3,2,-1},
            {3,2,1,-1},
            {1,1,-1,-2},
            {-1,-1,-2,-3}
        };
        int ans=countNegatives(grid);
        System.out.println(ans);
    }
    public static int countNegatives(int[][] grid) {
        int r=0; int c=grid[0].length-1;int count=0;
        while(r<=grid.length-1 && c>=0){
            if(grid[r][c]<= -1){
                count+=grid.length-r;
                c--;
            }
            else{
                r++;
            }
            
        }
        return count;
    }
}
