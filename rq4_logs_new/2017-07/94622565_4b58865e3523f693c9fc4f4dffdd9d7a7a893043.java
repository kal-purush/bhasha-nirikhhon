/**
 * Prints numbers from 0 to 100. All the numbers that are
 * multiples of 3 are replaced by 3*N, where N is the 
 * required multiplier, so as to obtain a replacement number.
 */
class TaskDevFirst {
  public static void main(String[] args) {
    final int START_COUNT = 0;
    final int END_COUNT = 100;
    final int FACTOR = 3;
		
    for (int i = START_COUNT; i <= END_COUNT; i++) {
      if (i % FACTOR == 0) {
        int temp = i/FACTOR;
        System.out.println(FACTOR + "*" + temp);
      } else {
        System.out.println(i);
      }
    }
  }
}