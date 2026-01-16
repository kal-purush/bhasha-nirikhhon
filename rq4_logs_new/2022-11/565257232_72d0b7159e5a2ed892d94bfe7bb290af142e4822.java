package maxProduce;

/**
 * @author Breethy
 * @create 2022/11/15 23:06
 * 线性查找
 */
public class GetMaxMin {
    public static int getMaxMin(int[] nums){
        int minF = Integer.MAX_VALUE,minS = Integer.MAX_VALUE;
        int maxF = Integer.MIN_VALUE,maxS = Integer.MIN_VALUE,maxT = Integer.MIN_VALUE;
        for (int num : nums) {
            if (num < minF){
                minS = minF;
                minF = num;
            }else if (num < minS){
                minS = num;
            }
            if (num > maxF){
                maxT = maxS;
                maxS = maxF;
                maxF = num;
            }else if (num > maxS){
                maxT = maxS;
                maxS = num;
            } else if (num > maxT) {
                maxT = num;
            }
        }
        return Math.max(minS*minF*maxF,maxF*maxS*maxT);
    }
}