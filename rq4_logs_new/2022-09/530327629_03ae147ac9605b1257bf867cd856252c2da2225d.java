public class SearchInRotatedSortedArray {
    public static void main(String[] args) {
        int[] arr={1,1,1,1,1,1,1,1,1,1,1,1,1,2,1,1,1,1,1};
        //{2,3,4,5,6,7,8,0,1}{4,5,6,7,0,1,2}
        //[1,1,1,1,1,1,1,1,1,1,1,1,1,2,1,1,1,1,1]

        System.out.println(search(arr,2));
        
    }
    public  static int search(int[] nums, int target) {
       int pivot= findPivotWithDuplicates(nums);
        if(pivot==-1){
         // that means array is not sorted circular array
         // do normal Binary Search
       return binarySearch(nums,target,0,nums.length-1);
        }
        else{
            //if pivot is found i have 2 asc sorted array
            if(target==nums[pivot]){
                //search in right
                return  pivot;
            }
             if(target>=nums[0]){
                //search in left
                return  binarySearch(nums, target,0,pivot-1);
            }
      
          
                return  binarySearch(nums, target,pivot+1,nums.length-1);
            
        //   
        }
    }

    public static int binarySearch(int[] arr, int target,int start,int end) {
        //stat,mid,end are main points of BS Algo
 
       
        while(start<=end){
            int mid= start + (end-start)/2;// int may go out of bound                   
            if(target>arr[mid]){
                // move right 
                start=mid+1;
            }
            if(target<arr[mid]){
                // move left
                end=mid-1;
            }
            else if(target==arr[mid]){
                return mid;
            }
        }

        return -1;
    }
    //pivot fun
    public static int findPivot(int[] arr){
        int start=0;
        int end=arr.length-1;
        while(start<=end){
            int mid=start+(end-start)/2;
            //finding  pivot (peak) element 
            //4-cases
            // case-1
            if(mid < end && arr[mid]>arr[mid+1]){
                return mid;
            }
            //case-2
            if(mid> start && arr[mid]<arr[mid-1]){
                return mid-1;
            }
            // case-3
         if(arr[mid]<= arr[start]){
            end =mid-1;
         } // case-4
         else{
            start=mid+1;
         }
         
        }
        
        return -1;
    }
    static int findPivotWithDuplicates(int[] arr) {
        int start = 0;
        int end = arr.length - 1;
        while (start <= end) {
            int mid = start + (end - start) / 2;
            // 4 cases over here
            if (mid < end && arr[mid] > arr[mid + 1]) {
                return mid;
            }
            if (mid > start && arr[mid] < arr[mid - 1]) {
                return mid-1;
            }

            // if elements at middle, start, end are equal then just skip the duplicates
            if (arr[mid] == arr[start] && arr[mid] == arr[end]) {
                // skip the duplicates
                // NOTE: what if these elements at start and end were the pivot??
                // check if start is pivot
                if (start < end && arr[start] > arr[start + 1]) {
                    return start;
                }
                start++;

                // check whether end is pivot
                if (end > start && arr[end] < arr[end - 1]) {
                    return end - 1;
                }
                end--;
            }
            // left side is sorted, so pivot should be in right
            else if(arr[start] < arr[mid] || (arr[start] == arr[mid] && arr[mid] > arr[end])) {
                start = mid + 1;
            } else {
                end = mid - 1;
            }
        }
        return -1;
    }


    
    // public static int findPivotInDuplicate(int[] arr){
    //     int start=0;
    //     int end=arr.length-1;
    //     while(start<=end){
    //         int mid=start+(end-start)/2;
    //         //finding  pivot (peak) element 
    //         //4-cases
    //         // case-1
    //         if(mid < end && arr[mid]>arr[mid+1]){
    //             return mid;
    //         }
    //         //case-2
    //         if(mid> start && arr[mid]<arr[mid-1]){
    //             return mid-1;
    //         }
    //         // case-3
    //        if(arr[start]==arr[mid]&& arr[mid]==arr[end]){
    //         //if start, mid,end are equal then skip start & end 
    //         //NOTE: what if start or end is pivot? check first
    //         if(start<end&&arr[start]>arr[start+1]){
    //             return start;
    //         }
    //         start++;
    //         if(end>start &&arr[end]<arr[end-1]){//my mistake
    //             return end-1;
    //         }
    //         end--;
    //        }
    //        // left side is sorted so, pivot should be in right side
    //        else if(arr[start]<arr[mid]|| (arr[start]==arr[mid]&&arr[mid]>arr[end])){
    //          //check in right side
    //          start=mid+1;
    //        }else{
    //         end=mid-1;
    //        }
         
    //     }
        
    //     return -1;
    // }

}