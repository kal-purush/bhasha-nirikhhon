package ua.lyashko.lesson5;

public class Practice_Task1 {
    public static void main ( String[] args ) {
        int[] heights = {120, -125, 135, -145, 150, -152, 112, -175, 123, -125,
                122, -135, 145, -154, 160, -183, 150, -130, 130, -119,
                130, -125, 123, -150, 123 };
        System.out.println ("boys avg: " + calcBoysAvgHeight ( heights ) );
        System.out.println ("girls avg: " + calcGirlsAvgHeight ( heights ) );
    }

   public static double calcBoysAvgHeight (int[] heights){
        return calculate ( heights, true );
   }

   public static double calcGirlsAvgHeight (int[] heights){
        return calculate ( heights, false );
   }

   public static double calculate ( int[] heights, boolean isBoy){
        int amount = 0;
        double sum = 0;
        for (int i : heights) {
            if ((i > 0 && !isBoy) || (i < 0 && isBoy)) {
                amount++;
                sum += i;
            }
        }
      return Math.abs ( sum / amount );
   }
}
