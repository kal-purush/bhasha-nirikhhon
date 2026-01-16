/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package labexercise7;
import java.util.ArrayList;

/**
 *
 * @author ARUMUGASAMY
 */
public class array {
    public static void main(String[] args) {
        int i, j, sum=0;
        double avg;
        int arr[]=new int[20];
        for(i=0; i<arr.length; i++)
        {
            arr[i]=(int)(Math.random()*90);
            sum=sum+arr[i];
            
        }
        avg=sum/arr.length;
        System.out.println("average"+avg);
        ArrayList<Integer>bavg=new ArrayList<Integer>();
        ArrayList<Integer>aavg=new ArrayList<Integer>();
        for(i=0; i<arr.length; i++)
            if(arr[i]<avg)
                bavg.add(arr[i]);
            else
                aavg.add(arr[i]);
        for(i=0; i<bavg.size(); i++)
            System.out.println(bavg.get(i)+" ");
        System.out.println();
        for(i=0; i<aavg.size(); i++)
            System.out.println(aavg.get(i)+" ");
        System.out.println();
    }
    
}