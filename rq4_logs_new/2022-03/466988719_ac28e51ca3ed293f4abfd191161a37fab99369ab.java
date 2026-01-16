package com.company;

import java.util.Scanner;

public class Tasks {

    public String longestPalidrome(String a){
        int n=a.length();
        String lb = null;
        int i,j,max=0,k;
        for(i=0;i<n;i++){
            for(j=i+1;j<n;j++){
                if(a.charAt(i)==a.charAt(j)){
                    String pl=a.substring(i,j+1);
                    String sb=a.substring(i,j+1);
                    String sbrev="";
                    for(int m=sb.length()-1;m>=0;m--)
                        sbrev+=sb.charAt(m);
                    if(pl.equals(sbrev)){
                        k=pl.length();
                        if(max<k) {
                            lb = pl;
                            max=pl.length();
                        }
                    }
                }
            }
        }
        return  lb;
    }

    public void armStrongNumber(int n){
        int l=n,c=0,k=0,ans=0,fin=n;

        while(l>0){
          l/=10;
          c++;
        }
        while(n>0){
           k=n%10;
            k=(int) Math. pow(k,c);
            ans+=k;
           n=n/10;
        }

        if(fin==ans){
           System.out.println("ArmStrongNumber");
        }
        else {
            System.out.println("Not a ArmStrongNumber");
        }

    }
    public static void main(String[] args){
        Tasks main = new Tasks();
        String lb=main.longestPalidrome("theehtquickbrownfoxxofnworbquickkciuq");
        System.out.println(lb);
        main.armStrongNumber(153);
        int n[]={12,34,5,23,78,23,9};
        main.findLargestArray(n);
        main.findSmithNumberOrNot(24);
        main.findDockNumber();

    }

    public void findDockNumber() {
        Scanner sc = new Scanner(System.in);
        System.out.print("Input a number : ");
        String nstr = sc.nextLine();

        int l = nstr.length();
        int count = 0;
        char chr;

        for(int i=1;i<l;i++)
        {
            chr = nstr.charAt(i);
            if(chr=='0')
                count++;
        }

        if(count>0 && nstr.charAt(0)!='0')
            System.out.println("Duck number");
        else
            System.out.println("Not a duck number");
    }

    public void findLargestArray(int[] n) {
        for(int i=0;i<n.length;i++){
            for(int j=i+1;j<n.length;j++){
                if(n[i]<n[j]){
                    int temp=n[i];
                    n[i]=n[j];
                    n[j]=temp;
                }
            }
        }
        System.out.println("Third Largest number: "+n[2]);
    }

    public void findSmithNumberOrNot(int number) {
        int pf=0,l=0,n1,pf1 = 0;
        int n2=number,n3=number;
        for(int i = 2; i<=number; i++) {
            while(number%i == 0) {
                n1=i;
               // System.out.println(i);
                while(n1>0){
                    l=n1%10;
                    pf+=l;
                    n1=n1/10;
                }
                number = number/i;
            }
        }
        if(number >2) {
            pf=number;
        }
        while(n2>0){
            int j=n2%10;
            pf1+=j;
            n2/=10;
        }
       if(pf==pf1){
           System.out.println("Smith number");
       }
       else{
           System.out.println("Not smith number");
       }
    }
}