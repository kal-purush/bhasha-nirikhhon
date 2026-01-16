package com.lxy.code;

import java.util.Vector;

public class CodeTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		double test = 0;
		int[] nums1 = {3};
		int[] nums2 = {1,2,4};
		test = findMedianSortedArrays(nums1, nums2);
		System.out.println(test);
	}
	public static double findMedianSortedArrays(int[] nums1, int[] nums2) {
		int m = nums1.length;
        int n = nums2.length;
        if(m>n) return findMedianSortedArrays(nums2, nums1);
        int min = 0;
        int max = m;
        int mid = (n + m + 1)/2;
        int i=0;
        int j=0;
        int count=0;
        while(max >=min){
        	count++;
            i = (min+max+1)/2;
            j = mid - i;
        	if(i>0 && j<n && nums1[i-1]>nums2[j]) max = i-1;
            else if(j>0 && i<m && nums2[j-1]>nums1[i]) min = i+1;
            else {
                if(i==0){
                   if((m+n)%2==1) {
                       return nums2[j-1];
                   }else{
                       if(m==n) return (double)(nums1[0]+nums2[n-1])/2;
                       return (double)(nums2[j-1]+Math.min(nums2[j],nums1[0]))/2;
                   }
                }else if(j==0){
                     if((m+n)%2==1) {
                       return nums2[j-1];
                     }else{
                       if(m==n) return (double)(nums1[m-1]+nums2[0])/2;
                       return (double)(nums2[j]+nums2[j-1])/2;
                     }
                  }
//                  else if(i==m){
//                    if((m+n)%2==1) {
//                       return nums2[j-1];
//                     }else{
//                       return (double)(nums2[j]+nums2[j-1])/2;
//                     }
//                }
                else break;
                
            }

        }
        System.out.print("i=" + i + " j=" + j + " mid" + mid );
        if((m+n)%2==1) return Math.max(nums1[i-1],nums2[j-1]);
        else{ 
        	if(i<m) return (double)(Math.min(nums1[i],nums2[j])+Math.max(nums1[i-1],nums2[j-1]))/2;
        	else return (double)(nums2[j]+Math.max(nums1[i-1],nums2[j-1]))/2;
        }
	}

}