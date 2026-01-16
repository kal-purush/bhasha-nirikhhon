import java.util.ArrayList;
import java.util.List;
/**
    228. Summary Ranges
    @author Brisa Castillo
    @version 12 June 2023 22:58 BOT
 */
public class SummaryRange {
    public static List<String> summaryRanges(int[] nums) {
        List<String> list = new ArrayList<>();
        if(nums.length == 0) {
            //list.add(null);
            return list;
        }else {
            int start = nums[0];
            int end = nums[0];

            for (int i = 1; i < nums.length; i++) {
                if (nums[i] != nums[i - 1] + 1) {
                    end = nums[i - 1];
                    if (start == end)
                        list.add(Integer.toString(start));
                    else
                        list.add(start + "->" + end);
                    start = nums[i];
                }
            }
            end = nums[nums.length - 1];
            if (start == end)
                list.add(Integer.toString(start));
            else
                list.add(start + "->" + end);
            return list;
        }
    }

    public static void main(String[] args) {
        int[] nums = {0,2,3,4,6,8,9};
        int[] nums2 = {0,1,2,4,5,7};
        int[] empty = {};
        System.out.println(summaryRanges(empty));
    }
}