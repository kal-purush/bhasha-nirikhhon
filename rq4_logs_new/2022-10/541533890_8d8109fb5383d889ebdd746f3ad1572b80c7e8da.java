package IMPORTANT;

public class BOOKS_ALLOCATION {
    public static void main(String[] args) {
        int []days={1,2,2,3,1};
        int no_of_days=3;

        System.out.println(allocation(days,no_of_days));

    }
    static int allocation(int [] arr,int m)
    {
        int max=0;
        int sum=0;
        for(int val:arr)
        {
            if(val>max)
            {
                max=val;
            }
            sum=sum+val;
        }
        System.out.println(sum+" "+max);
        int lo=max;
        int high=sum;
        int ans=0;
        System.out.println(lo+" "+high);
        while(lo<=high)
        {
            int mid=lo+(high-lo)/2;
            if(isPossible(arr,mid,m)==true)
            {
                ans=mid;
                high=mid-1;
            }
            else
            {
                lo=mid+1;
            }
        }

        return ans;
    }
static Boolean isPossible(int [] arr,int load,int days)
{
    int day=1;
    int ans=0;
    for(int i=0;i< arr.length;i++)
    {
        ans+=arr[i];
        if(ans>load)
        {
            day++;
            ans=arr[i];
        }
    }

    return day <= days;
}

}