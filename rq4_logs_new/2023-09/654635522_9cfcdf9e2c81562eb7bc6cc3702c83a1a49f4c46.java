public class prob11 {
    public static void main(String[] args) {
        // amazon question.
        //Given a " 2 x n" floor and tiles of size of "2 x 1" , count the number of 
        // count the number of ways to tile the given floor using the tiles.
        // a tile can either be placed hotizontally or vertically.
        System.out.println(tilingProblem(3));
    }
    // for n=0. n=1,n=2,n=3... find the arrangements of placing the tiles , and finnaly to have the total possiblilities add the horixontal and the vertical way .(like P & C)
    public static int tilingProblem(int n){
        // 2 x  n floor size

        // base case 
        if(n==0 || n==1) return 1;

        // vertical way
        int vertical = tilingProblem(n-1);

        // horizontal
         int horizontal = tilingProblem(n-2);

         return horizontal+vertical;

    }
}