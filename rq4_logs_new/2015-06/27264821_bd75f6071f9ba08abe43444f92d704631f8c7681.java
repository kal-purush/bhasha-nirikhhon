import java.util.Collections;


public class FindArrayElement {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//                      0  1  2  3  4  5
		int[] arr = new int[] { 1, 2, 3, 4, 5, 10 };
		System.out.println( solution(arr, 0) );
//		Collections.binarySearch(list, key)

	}
	
	/**
	 	0  1  2  3  4
	 	1, 2, 5, 9, 9
	 	l     m
	 	            r
	 	            	 	            
	 	1, 2, 3, 5, 9
	 	 	     l  r
	 	         
	 
	 N:	5
	 l:	0
	 r:	4
	 */
	
	static int solution(int[] A, int X) {
        int N = A.length;
        if (N == 0) {
            return -1;
        }
        int l = 0;
        int r = N - 1;
        while (l < r) {
            int m = (l + r) / 2;
            if (A[m] > X) {
                r = m - 1;
            } else {
            	if( A[m] == X )
            		return m;
                l = m+1;
            }
        }
        if (A[l] == X) {
            return l;
        }
        return -1;
    }
	
//	static int solution(int[] A, int X) {
//        int N = A.length;
//        if (N == 0) {
//            return -1;
//        }
//        int l = 0;
//        int r = N - 1;
//        while (l < r) {
//            int m = (l + r) / 2;
//            if (A[m] > X) {
//                r = m - 1;
//            } else if(A[m] < X) {
//                l = m + 1;
//            } else if( A[m] == X )
//            	return m;
//        }
//        if (A[l] == X) {
//            return l;
//        }
//        return -1;
//    }

}