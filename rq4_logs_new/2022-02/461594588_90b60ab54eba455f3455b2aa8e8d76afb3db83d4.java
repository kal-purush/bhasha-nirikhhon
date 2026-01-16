package algos.dp.NumberOfCoins;

// { Driver Code Starts
//Initial Template for Java

import java.util.*;
class GfG
{
    public static void main(String args[])
    {
        Scanner sc = new Scanner(System.in);
        int t = sc.nextInt();
        while(t-->0)
        {
            int v = sc.nextInt();
            int m = sc.nextInt();
            int coins[] = new int[m];
            for(int i = 0;i<m;i++)
                coins[i] = sc.nextInt();
            Solution ob = new Solution();
            System.out.println(ob.minCoins(coins, m, v));
        }
    }
}    // } Driver Code Ends


class Solution{

    static int[][] arr;
    public int minCoins(int coins[], int M, int V)
    {
        arr = new int[M+1][V+1];
        for(int i =0;i<M+1;i++){
            arr[i][0]=0;
            for(int j = 0; j<V+1;j++){
                if(i==0){
                    if(j!=0)
                        arr[i][j]=-1;
                    continue;
                }
                int origIdx=i-1;
                if(coins[origIdx]<=j){
                    int minCoinsFromleftOverSum = findMin(arr,i,j-coins[origIdx]);
                    if (minCoinsFromleftOverSum!=-1){
                        if(arr[i-1][j]!=-1)
                            arr[i][j]=Math.min((1+minCoinsFromleftOverSum),1+arr[i-1][j]);
                        else
                            arr[i][j]=1+minCoinsFromleftOverSum;
                    }
                    else
                        arr[i][j]=arr[i-1][j];
                }else{
                    arr[i][j]=arr[i-1][j];
                }
            }
        }
        print2DMatrix(arr,M,V);
        return findMin(arr,M,V);
    }

    private void print2DMatrix(int[][] arr,int M, int V) {
        for(int i =0;i<V+1;i++)
            System.out.printf("%3s",i);
        for(int i =0;i<M+1;i++) {
            System.out.println();
            for (int j = 0; j < V + 1; j++) {
                System.out.printf("%3s",arr[i][j]);
            }
        }
        System.out.println();
    }

    public int findMin(int[][] arr, int row,int column){
        int minNumber=Integer.MAX_VALUE;
        int cur=-1;
        for(int i = 0;i<row+1;i++) {
            cur = arr[i][column];
            if ((cur!=-1) && (cur<minNumber))
                minNumber = cur;
        }
        return minNumber==Integer.MAX_VALUE?-1:minNumber;
    }
}