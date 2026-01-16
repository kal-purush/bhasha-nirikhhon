package Array;

public class RemoveElement_27 {

  public int removeElement(int[] nums, int val) {
    var fast = 0;
    var slow = -1;
    while (fast < nums.length) {
      if (nums[fast] == val) {
        if (slow == -1) {
          slow = fast;
        }
      } else {
        if (slow != -1) {
          nums[slow] = nums[fast];
          slow++;
        }
      }
      fast++;
    }
    return slow == -1 ? fast : slow;
  }

  public static void main(String[] args) {
//    var nums = new int[]{3, 2, 2, 3};
//    var val = 3;
//    var nums = new int[] {0,1,2,2,3,0,4,2};
//    var val = 2;
//    var nums = new int[]{};
//    var val = 0;
    var nums = new int[]{4, 5};
    var val = 4;
    System.out.println(new RemoveElement_27().removeElement(nums, val));
  }
}