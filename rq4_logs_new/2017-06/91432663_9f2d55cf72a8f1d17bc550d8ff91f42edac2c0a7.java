package com.jason.test.testapp.java.recursion;

/**
 * @Description: [�۰������ʹ�õ��ĵݹ�ͷ���˼��, O(logN)]
 * @Author: [�ֻ�]
 * @CreateDate: [2014-4-1 ����1:24:01]
 * @CsdnUrl: [http://blog.csdn.net/ljphhj]
 */
public class BinarySearch {

    public static boolean IsExist(int[] arrays, int key, int left, int right) {
        /*�ݹ��������*/
        if (left == right) {
            if (arrays[left] == key) {
                return true;
            } else {
                return false;
            }
        }
        /*�ݹ����*/
        int middle = (right + left) / 2;
        if (arrays[middle] > key) {
            return IsExist(arrays, key, left, middle - 1);
        } else if (arrays[middle] < key) {
            return IsExist(arrays, key, middle + 1, right);
        } else {
            return true;
        }
    }

    public static void main(String[] args) {
        int[] arrays = new int[]{-2, -3, 1, 2, 3, 4, 5, 6, 7, 10};
        System.out.println(BinarySearch.IsExist(arrays, -3, 0, arrays.length - 1));
    }
}