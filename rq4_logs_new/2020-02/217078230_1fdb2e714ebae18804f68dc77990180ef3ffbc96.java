import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Day13 {
    //子集 没有重复元素
    public static List<List<Integer>> subsets(int[] nums) {
        List<List<Integer>>ret=new ArrayList<>();
        ret.add(new ArrayList<>());
        for(int i=0;i<nums.length;i++){
            int sz=ret.size();
            for(int j=0;j<sz;j++){
                List<Integer>tmp=new ArrayList<>(ret.get(j));
                tmp.add(nums[i]);
                ret.add(tmp);
            }
        }
        return  ret;
    }
    ////子集 有重复元素
    public static List<List<Integer>> subsetsWithDup(int[] nums) {
        List<List<Integer>>ret=new ArrayList<>();
        ret.add(new ArrayList<>());
        Arrays.sort(nums);
        List<Integer>list=new ArrayList<>();
        list.add(nums[0]);
        ret.add(list);
        if(nums.length==1){
            return ret;
        }
        int lastLen=1;
        for(int i=1;i<nums.length;++i){
            int sz=ret.size();
            if(nums[i]!=nums[i-1]){
                lastLen=sz;
            }
            for(int j=sz-lastLen;j<sz;j++){
                List<Integer>tmp=new ArrayList<>(ret.get(j));
                tmp.add(nums[i]);
                ret.add(tmp);
            }
        }
        return ret;
    }

    public static void main(String[] args) {
        int []nums={1,2,3};
        int []nums1={1,2,2};
        System.out.println(subsets(nums));
        System.out.println(subsets(nums1));
        System.out.println(subsetsWithDup(nums));
        System.out.println(subsetsWithDup(nums1));
    }
}