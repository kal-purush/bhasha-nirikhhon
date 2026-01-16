public class Q120 {
  public static void main(String[] args) {
    final int LIMIT = 1000;

    int sumMaxRemainder = 0;

    for (int i = 3; i <= LIMIT; i++) {
        if (i % 2 == 0) {
            sumMaxRemainder += i * (i - 2); 
        } else {
            sumMaxRemainder += i * (i - 1);
        }
    }
    
    
    System.out.println(sumMaxRemainder);
  }
}