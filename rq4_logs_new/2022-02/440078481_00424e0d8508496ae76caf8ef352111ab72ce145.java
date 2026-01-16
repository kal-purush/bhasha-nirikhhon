package FastSlowPointer.Problems;

public class Problem3 {
    public Problem3(){
        int[] arr = new int[]{1, -2, 1, 2};
        
        System.out.println(isCircular(arr));
    }

    public boolean isCircular(int[] arr){
        int edge = arr.length - 1;

        for(int start = 0; start <= edge; start++){
            int slow = start;
            int fast = start+1;

            while(fast >= 0 && fast <= edge && slow != fast){
                slow += arr[slow];
                
                fast += arr[fast];
                if(fast < 0 || fast > edge){
                    return true;
                }
                fast += arr[fast];   
            }
            if(fast < 0 || (fast > edge && slow != edge)){
                return true;
            }
        }

        return false;
    }
}