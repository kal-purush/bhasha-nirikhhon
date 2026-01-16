/*Given an array of the size n-1 
containing distinct integers in the range of 1 to n;
have to find the missing number.
 */
import java.util.Scanner;
class MissingNumberFinder {
 public static void main(String[] args) 
 {
 Scanner in = new Scanner(System.in);
 int size = in.nextInt();
 int[] arr = new int[size-1];
 for(int i=0;i<size-1;i++)
 arr[i] = in.nextInt();
 System.out.println(missingNumberFinder(arr, size));
 in.close();
 }
 static int missingNumberFinder(int array[], int n) 
 {
 // my code begins here.
 /* I will check from i=1 to n/2 
 if either i or n-(i-1) is not present in entire array
 I will return whichever not present */
int missingNumber=-1;
 for(int i=1;i<=n/2;i++)
{
    boolean f=false,g=false; // f for tracking i present, g for n-(i-1)
    for(int ind=0;ind<(n-1);ind++)
    {
        if(array[ind]==i)
        f=true;
        if(array[ind]==n-(i-1))
        g=true;

    }
    if (f==false)
    {
    missingNumber=i;
    break;
    }
    if(g==false)
    {
    missingNumber=n-(i-1);
    break;
    }
}
return missingNumber;  
}
}
/*Input:
6
1
3
2
6
5
Output:
4 
 */