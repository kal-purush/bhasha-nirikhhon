package com.tie.algo.sort;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class BubbleSort {

    //ð������ a��ʾ���飬n��ʶ�����С
    public static void bubbleSort(int[] a, int n) {
        if(n <= 1) return;

        for(int i = 0; i < n; i++) {
            //��ǰ�˳�ð��ѭ����־λ
            boolean flag = false;
            for(int j = 0; j < n - i - 1; j++) {
                if(a[j] > a[j + 1]){
                    int temp = a[j + 1];
                    a[j + 1] = a[j];
                    a[j] = temp;
                    //��ʶ�����ݽ���
                    flag = true;
                }
            }
            if (!flag) {
                break;  //��ǰ�˳�
            }
        }
    }


    public static void main(String[] args) {
        int[] a = {6,3,2,5,7,5,1};
        bubbleSort(a, a.length);

        for (Integer integer : a) {
            System.out.println(integer);
        }
    }
}