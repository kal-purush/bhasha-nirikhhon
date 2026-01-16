package nums;

import java.util.Arrays;

/**
 *  需求：两个有序数组nums和nums2将nums2合并到nums1中，使得nums1成为一个有序数组
 *
 *
 */
public class Mergesz {
    public static void main(String[] args) {
        int [] nums1 = {1,2,3,4,8,0,0,0};
        int [] nums2 = {2,3,4};
        merge(nums1,5,nums2,3);
        for ( int i: nums1
             ) {
            System.out.println(i);
        }
    }

    /**
     * 假设nums1的空间大小等于m+n就可以有足够的空间
     * 合并后再排序
     * @param nums1
     * @param m 已经排好序的元素的个数
     * @param nums2
     * @param n
     */
    public static void merge(int [] nums1,int m,int [] nums2,int n){
        System.arraycopy(nums2,0,nums1,m,n);
        Arrays.sort(nums1);//返回的还是nums1
    }

    /**
     * 利用双指针，从后往前
     * @param nums1
     * @param m
     * @param nums2
     * @param n
     */
    public static void mergeDouble(int [] nums1,int m,int [] nums2,int n){
        //指针先指向最后一个元素的索引,然后慢慢向前移动，当索引<0时结束循环
        int p1 = m-1;
        int p2 = n-1;
        int p=m+n-1;
        while ((p1>=0)&&(p2>=0)){
            nums1[p--] = (nums1[p1]<nums2[p2])? nums2[p2--]:nums1[p1--];
        }
        //如果两个数组的长度不一致
        /**
         * 退出循环之后，其中一个数组必定排序完成，剩余数组的元素本就是排序好的数组，只需将数组2中剩余的元素直接copy
         * 如果剩余的是p2,直接copy到最前面，如果剩余的是p1就不需要再排了，（因为本来就在数组一中操作）
         */
        System.arraycopy(nums2,0,nums1,0,p2+1);//最后一个参数代表的是元素的个数，所以要索引+1

    }
}