package LeeCode;

public  class test {

    public static void main(String[] args) {
        try {
            int a = 2 / 0;
        } catch (Exception e) {
            System.out.println("ss");
        }
    }
}
package 网易;

public class text extends Thread{
    @Override
    public void run() {

    }
}
package 网易;

import java.util.Scanner;

public class zuid {
    public static void main(String[] args) {
        Scanner s=new Scanner(System.in);
        int n=s.nextInt();
        int nums[]=new int[n];
        for(int i=0;i<nums.length;i++){
            nums[i]=s.nextInt();
        }
        int sum=0;
        for(int i=0;i<n;i++){
            for(int j=i+1;j<n;j++){
                if(nums[j]<nums[i]){
                    sum=sum+j-i;
                }
            }
        }
        System.out.println(sum);
    }
}
package 网易;

import java.util.Scanner;

/**
 * 2
 * 5
 * 1 3 9 2 6
 * 5
 * 4 2 9 16 7
 *
 * 3
 * 3
 */
public class 完美的序列 {
    public static void wangmei(int n,String str){
        String []strings=str.split(" ");
        int length=strings.length;
        int []nums=new int[length];
        for(int i=0;i<length;i++){
            nums[i]=Integer.parseInt(strings[i]);
        }
        //数据有了 ,dp[i]表示第I个元素的最长完美长度之和
        //每个元素最为一次
        int max=0;

        for(int j=1;j<length;j++){
            int dp[]=new int[length];
            int count=1;
            dp[j-1]=nums[j-1];
            for(int i=j;i<length;i++){
                if(nums[i]<dp[i-1]){
                    dp[i]=nums[i];
                    if(i>length/2){
                        break;
                    }
                }else {
                    dp[i]=dp[i-1]+nums[i];
                    count++;
                }
            }
            if(max<count){
                max=count;
            }
        }
        System.out.println(max);

    }
    public static void main(String[] args) {
        Scanner s=new Scanner(System.in);
        //总条数
        int n=Integer.parseInt(s.nextLine());
        String[]str1=new String[n];
        String[]strings=new String[n];
        for(int i=0;i<n;i++){
            str1[i]=s.nextLine();
            strings[i]=s.nextLine();
        }
        for (int i=0;i<n;i++){
            int total=Integer.parseInt(str1[i]);
            String str=strings[i];
            wangmei(total,str);
        }


    }
}
package 网易;

import java.util.Scanner;

public class 最小数位和 {

    public static String printNum(int n){
        if(n<10){
            return String.valueOf(n);
        }
        int count=n/9;
        int countwei=n%9;
        StringBuilder stringBuilder=new StringBuilder();
        if(countwei!=0){
            stringBuilder.append(countwei);
        }
        for(int i=0;i<count;i++){
            stringBuilder.append(9);
        }

        String s=stringBuilder.toString();
        return s;
    }
    public static void main(String[] args) {
        Scanner s=new Scanner(System.in);
        int n=s.nextInt();
        int []nums=new int[n];
        for(int i=0;i<n;i++){
            nums[i]=s.nextInt();
        }
        for(int i=0;i<n;i++){
            System.out.println(printNum(nums[i]));
        }
    }
}
package 网易;

import java.util.Scanner;

public class 翻倍 {

    public static int mindo(String str){
        //这个数组包含了四个数,让b<=a 1 5 7 2
        String []strings=str.split(" ");
        Long a=Long.parseLong(strings[0]);
        Long b=Long.parseLong(strings[1]);
        Long p=Long.parseLong(strings[2]);
        Long q=Long.parseLong(strings[3]);
        int count=0;
        while (b>a){
            if(a+p>=b){
                a=a+p;
                count++;
            }else {
                p=p*q;
                count++;
            }
        }
        return count;
    }
    public static void main(String[] args) {
        Scanner in=new Scanner(System.in);
        int n=Integer.parseInt(in.nextLine());
        String[]strings=new String[n];
        for(int i=0;i<n;i++){
           strings[i]=in.nextLine();
        }
        for(int i=0;i<n;i++){
            System.out.println(mindo(strings[i]));
        }
    }
}