
public class DiscIntersection {

	public static void main(String[] args) {
	   int A[] = new int[]{1, 5, 2, 1, 4, 0};

	   System.out.println(solution(A));
	}
	public static int solution(int[] A) {
		int count=0;
		for(int i=0;i<A.length;i++){
			long x1, y1;
				x1=i+A[i];
				y1=i-A[i];
			for(int j=i+1; j<A.length;j++){
				long x2, y2;
				x2=j+A[j];
				y2=j-A[j];
				
				if(isOverlapping(y1, x1, y2, x2)){
					System.out.println(i+ " "+ j);
					count++;
				}
			}
		}
		if (count>10000000) return -1;
		return count;		
	}
	
	public static boolean isOverlapping(long x1, long y1, long x2, long y2){
		return (x1<=y2 && x2<=y1);
	}
}