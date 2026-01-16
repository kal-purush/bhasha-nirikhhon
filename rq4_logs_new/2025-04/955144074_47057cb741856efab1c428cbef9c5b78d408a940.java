package begayim.recursion1;

public class rec_16_17_18 {

    public int array11(int[] nums, int index) {
        if (index >= nums.length) {
            return 0;
        }
        int count = 0;
        if (nums[index] == 11) {
            count += 1;
        }
        return count + array11(nums, index + 1);
    }

    public boolean array220(int[] nums, int index) {
        if (index >= nums.length - 1) {
            return false;
        }
        if (nums[index] * 10 == nums[index + 1]) {
            return true;
        }
        return array220(nums, index + 1);
    }

    public String allStar(String str) {
        if (str.length() <= 1) {
            return str;
        }
        return str.charAt(0) + "*" + allStar(str.substring(1));
    }
}