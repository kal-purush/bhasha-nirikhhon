import java.math.BigInteger;

class GetAverages {
    public int[] getAverages(int[] nums, int k) {
        if(k == 0)
            return nums;
        else if(nums.length == 1){
            nums[0] = -1;
            return nums;
        }else if (nums.length == k || nums.length < (k*2) + 1 ){
            int[] result = new int[nums.length];
            for(int i=0; i<result.length; i++){
                result[i] = -1;
            }
            return result;
        }else {
            int[] result = new int[nums.length];
            int dividend = (k*2)+1;
            BigInteger avg = BigInteger.ZERO;
            int mid = k;
            int first = -1;
            int last = k*2;
            for(int i=0; i < dividend; i++){
                avg = avg.add(BigInteger.valueOf(nums[i]));
                //System.out.print(nums[i] + " ");
            }
            BigInteger r = avg.divide(BigInteger.valueOf(dividend));
            result[k] = r.intValue();
            //result[k] = avg.intValue()/dividend;
            for(int i=0; i<nums.length; i++){
                if(i-k < 0 || i+k > nums.length - 1)
                    result[i] = -1;
                else{
                    if(i != k){
                        last++;
                        first++;
                        //System.out.println(first + "-" + last);
                        avg = avg.add(BigInteger.valueOf(nums[last]));
                        avg = avg.subtract(BigInteger.valueOf(nums[first]));
                        //avg += nums[last];
                        //avg -= nums[first];
                        //result[i] = avg.intValue()/dividend;
                        BigInteger s = avg.divide(BigInteger.valueOf(dividend));
                        result[i] = s.intValue();
                    }
                    /*Integer[] subarray = IntStream.range(i-k, (i-k)+(k*2) + 1)
                                .mapToObj(j -> nums[j])
                                .toArray(Integer[]::new);
                                System.out.println(Arrays.toString(subarray));*/

                    //result[i] = Arrays.stream(subarray).average().getAsInteger();

                    //System.out.println();
                    //avg /= ((k*2)+1);
                    //result[i] = avg;
                }
            }
            return result;
        }
    }
}