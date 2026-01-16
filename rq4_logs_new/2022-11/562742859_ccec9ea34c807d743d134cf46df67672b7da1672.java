public class coincidental_cohesion {
  // 2 unrelated methods are placed together in a single class. Thus there is coincidental cohesion.
  public int getLargest(int arr[]) {
    int max = arr[0];
    for(int i = 1; i < arr.length; i++) {
      if (arr[i] > max) {
        max = arr[i];
      }
    }
    return max;
  }

  public void palindrome(String input) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(input);
    stringBuilder.reverse();
    String revString = new String(stringBuilder);
    if(input == revString) {
      System.out.println(input + " is a palindrome!");
    }
    else {
      System.out.println(input + " is not a palidrome.");
    }
  }

  public static void main(String []args) {
    int arr[] = {1, 2, 3, 4, 5};
    coincidental_cohesion obj = new coincidental_cohesion();
    int max = obj.getLargest(arr);
    System.out.println("Largest value: " + max);
    obj.palindrome("nitin");
  }
}