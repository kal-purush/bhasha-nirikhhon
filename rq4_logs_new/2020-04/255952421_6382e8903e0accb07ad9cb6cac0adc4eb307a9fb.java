package com.thread.fork;

import java.util.Random;

/**
 * 构造一个数组
 * @Date 2020/4/23 18:58
 * @name MakeArray
 */


public class MakeArray {

    public static final  int ARRAY_LENGTH=4000000;

    public final static int THRESHOLD=47;
    public static int[] makeArray() {

        //new一个随机数发生器
        Random r = new Random();
        int[] result = new int[ARRAY_LENGTH];
        for(int i=0;i<ARRAY_LENGTH;i++){
            //用随机数填充数组
            result[i] =  r.nextInt(ARRAY_LENGTH*3);
        }
        return result;
    }

    public static void main(String[] args) throws InterruptedException {
        Long count =0L;
        int[] src = MakeArray.makeArray();
        long start = System.currentTimeMillis();
        for(int i= 0;i<src.length;i++){
            count = count + src[i];
        }
        System.out.println("The count is "+count
                +" spend time:"+(System.currentTimeMillis()-start)+"ms");
    }
}