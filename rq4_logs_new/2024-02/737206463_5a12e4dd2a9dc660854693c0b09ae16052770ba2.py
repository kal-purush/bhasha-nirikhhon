class Solution:
    def specialArray(self, nums: List[int]) -> int:
        #def lower_bound(arr,x):
        #    low,high=0,len(arr)-1
        #    ans=len(arr)
        #    while low<=high:
        #        mid=(low+high)//2
        #        if arr[mid]==x:
        #            ans=mid
        #            high=mid-1
        #        elif arr[mid]>x:
        #            high=mid-1
        #        else:
        #            low=mid+1
        #    return (len(arr)-1-ans) if ans!= -1 else len(arr)-1 -1
        #nums.sort()
        #n=len(nums)
        #l=[]
        #for i in range(n+1):
        #    op=lower_bound(nums,i)
        #    if op!= i:
        #        pass
        #    else:
        #        l.append(i)
        #return max(l) if len(l)!=0 else -1
        def lower_bound(arr,x):
            low,high=0,len(arr)-1
            ans=len(arr)
            while low<=high:
                mid=(low+high)//2
                if arr[mid]>=x:
                    ans=mid
                    high=mid-1
                else:
                    low=mid+1
            if ans==len(arr):
                return -1
            else:
                return ans
        def upper_bound(arr,x):
            low,high=0,len(arr)-1
            ans=len(arr)
            while low<=high:
                mid=(low+high)//2
                if arr[mid]>x:
                    ans=mid
                    high=mid-1
                else:
                    low=mid+1
            return ans
        nums.sort()
        #first=lower_bound(nums,x)
        n=len(nums)
        for i in range(n+1):
            first=n-lower_bound(nums,i)
            if first==i:
                return i
        return -1    



        
             

            
