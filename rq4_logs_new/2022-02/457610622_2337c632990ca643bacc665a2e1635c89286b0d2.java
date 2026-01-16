package com.example.myapplication.algorithm;

import android.util.Log;

import java.util.Arrays;

/**
 * 算法工具类
 */
public class AlgorithmUtils {

    public static final String TAG = AlgorithmUtils.class.toString();

    /**
     * 冒泡排序
     * 将数据前一个与后一个进行对比，大的后移，最终无变化时输出，用一个flag表示如果一趟无交换，
     * 即可输出最终排序结果
     *
     * @param arr 源数据
     */
    public static void bubblingSort(int arr[]) {
        int temp;
        boolean isRight = false;
        for (int i = arr.length - 1; i > 0; i--) {
            for (int j = 0; j < i; j++) {
                if (arr[j] > arr[j + 1]) {
                    temp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = temp;
                    isRight = true;
                }
            }
            if (!isRight) {
                break;
            } else {
                isRight = false;
            }
        }

        Log.d(TAG, "bubblingSort: the last arr is " + Arrays.toString(arr));
    }

    /**
     * 选择排序
     * 定义：每次取这段数组中的最小数（或者最大数）将其与第一个数交换，然后刨除这个数，
     * 再剩下的数中再次选取最小值（最大值）,直到排序完成
     * if arr[0]-arr[n] 中最小为arr[3],然后将arr[3]与arr[0]交换，然后在arr[1]-arr[n]之间取最小值与arr[1]交换,
     * 以此类推
     *
     * @param arr
     */
    public static void selectSort(int arr[]) {
        int min;
        int index;
        for (int i = 0; i < arr.length - 1; i++) {
            min = arr[i];
            index = i;
            for (int j = i + 1; j < arr.length; j++) {
                if (min > arr[j]) {
                    min = arr[j];
                    index = j;
                }
            }
            if (arr[i] != min) {
                arr[index] = arr[i];
                arr[i] = min;
            }
        }

        Log.d(TAG, "selectSort: the last sort is " + Arrays.toString(arr));
    }

    /**
     * 插入排序
     * 方法：将数组分为两部分，前段是排序完的，后段是乱序的，每次从后端乱序的头一个取出插入前段有序
     * 选择合适位置
     * 如：{3,1,7,5,9,10}-{3},{1,7,5,9,10}-{1,3},{7,5,9,10}....
     *
     * @param arr
     */
    public static void insertSort(int[] arr) {
        int temp;
        boolean isSet = false;
        frist:
        for (int i = 1; i < arr.length; i++) {
            temp = arr[i];
            second:
            for (int j = i - 1; j >= 0; j--) {
                if (temp < arr[j]) {
                    arr[j + 1] = arr[j];
                } else {
                    arr[j + 1] = temp;
                    isSet = true;
                    break second;//跳出循环如果想内层跳出外层可以给循环命名，此处指跳出内层名不命名无所谓
                }
            }
            if (isSet) {
                isSet = false;
            } else {
                arr[0] = temp;
            }
        }
        Log.d(TAG, "insertSort: the last sort is " + Arrays.toString(arr));
    }

    /**
     * 插入排序之while循环法
     *
     * @param arr 源数据
     */
    public static void insetSortWhile(int arr[]) {
        int temp;//记录需要插入的数据
        int index;//需要插入的位置
        for (int i = 1; i < arr.length; i++) {
            index = i;
            temp = arr[i];
            while (index > 0 && temp < arr[index - 1]) {
                index--;
                arr[index] = arr[index - 1];
            }
            arr[index] = temp;
        }

        Log.d(TAG, "insetSortWhile: the last sort is "+Arrays.toString(arr));

    }

    /**
     * 插入排序while循环优化版
     */
    public static void insertSortWhilePerfert(int arr[]) {
        int temp;//记录需要插入的数据
        int index;//需要插入的位置
        for (int i = 1; i < arr.length; i++) {
            if (arr[i] < arr[i - 1]) {
                index = i;
                temp = arr[i];
                while (index > 0 && temp < arr[index - 1]) {
                    index--;
                    arr[index] = arr[index - 1];
                }
                arr[index] = temp;
            }
        }
        Log.d(TAG, "insertSortWhilePerfert: the last sort is "+Arrays.toString(arr));
    }

    /**
     * 希尔排序
     * 定义:将一组数据分割成lengths/2组，然后每组之内进行插入排序，然后再分割成lengths/2/2组，进行相同的操作，
     * 直到分成最后一组，再进行插入排序。
     * 插入排序在对几乎已经排好序的数据操作时，效率高，即可以达到线性排序的效率。希尔排序就是对插入排序的不稳定优化
     */
    public static void shellSort(int arr[]) {
        int group = arr.length;
        int temp;
        int index;
        while (group!=1){
            group/=2;
            for (int i = 0;i<group;i++){
                for(int j = i ; j<arr.length-group;j+=group){
                    index = j;
                    while (index>=0&&arr[index]>arr[index+group]){
                        temp = arr[index];
                        arr[index] = arr[index+group];
                        arr[index+group] = temp;
                        index-=group;
                    }

                }
            }
        }

        Log.d(TAG, "shellSort: the last sort is "+ Arrays.toString(arr));
    }

    /**
     * 希尔排序，插入排序法
     * 有问题
     */
    public static void shellSortNotChange(int[] arr){
        int group = arr.length;
        int temp;
        int index;
        while (group!=1){
            group/=2;
            for (int i = 0 ; i <group;i++){
                for(int j = i;j<arr.length-group;j+=group){
                    index = j;
                    temp = arr[index];
                    while (index>=0&&arr[index]>arr[index+group]){
                        arr[index+group] = arr[index];
                        index-=group;
                    }
                    arr[index] = temp;
                }
            }
        }
    }
}