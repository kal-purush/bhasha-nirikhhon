package bridgeLabz;

public class MaximumGeneric <E>{
	public static <E extends String> String compare(E x,E y ,E z) {
		E max;
		if(x.length() > y.length()) {
			if(x.length() > z.length()) {
				
				max = x;
			}else {
				
				max = z;
			}
		}else {
			if(y.length() > z.length()) {
				max = y;
			}else {
				max = z;
			}
		}
		
		return max;
		 
	 }
	
	public static <E extends Integer> int compare(E x,E y ,E z) {
		E max;
		if(x > y) {
			if(x > z) {
				
				max = x;
			}else {
				
				max = z;
			}
		}else {
			if(y > z) {
				max = y;
			}else {
				max = z;
			}
		}
		
		return max;
		 
	 }

	public static<E extends Float> float compare(float x, float y, float z) {
		float max;
			if(x > y) {
				if(x > z) {
					
					max = x;
				}else {
					
					max = z;
				}
			}else {
				if(y > z) {
					max = y;
				}else {
					max = z;
				}
			}
			
		return max;
	}
}