package lintcode;
public class MedianOfTwoSortedArrays
{
    public static int findK(int[] A, int[] B, int k, int aStart, int aEnd, int bStart, int bEnd)
    {
    	if (aStart > aEnd)
    		return B[bStart+k-1];
    	if (bStart > bEnd)
    		return A[aStart+k-1];
    	
    	
        if (k==1)
            return Math.min(A[aStart], B[bStart]);
        int mid = k / 2;
        int aMid, bMid;
        if (aEnd - aStart +1 <= mid)
            aMid = aEnd;
        else
            aMid = aStart+mid-1;
            
        if (bEnd - bStart +1 <= mid)
            bMid = bEnd;
        else
            bMid = bStart+mid-1;
            
        if (A[aMid] > B[bMid])
            return findK(A, B, k-mid, aStart, aEnd, bMid+1, bEnd);
        else
            return findK(A, B, k-mid, aMid+1, aEnd, bStart, bEnd);
    }
    
    public static double  findMedianSortedArrays(int[] A, int[] B) 
    {
        int n = A.length + B.length;
        double res;
        if (n%2 == 0)
        {
        	res = findK(A,B,n/2,0,A.length-1,0,B.length-1);
        	res += findK(A,B,n/2+1,0,A.length-1,0,B.length-1);
        	return res/2;
        }
        else
        {
        	res = findK(A,B,n/2+1,0,A.length-1,0,B.length-1);
        	return res;
        }
        
    }
    public static void main(String[] args)
	{
		int[] A = {1,2, 3};
		int[] B = {2,4,6,8,10};
		System.out.println(findMedianSortedArrays(A,B));
	}
}