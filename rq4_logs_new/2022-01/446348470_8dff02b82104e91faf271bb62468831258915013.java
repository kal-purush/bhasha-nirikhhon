package Algorithm;

import java.util.Arrays;
import java.util.List;

//배열 Index값 큰 순서
public class IndexOrder {

	public static void main(String args[]) {
		Integer[] age = {6,22,6};
		
		int answer[] = maxIndex(age);
			
		System.out.println(Arrays.toString(answer));
		
		
	}
	
	public static int[] maxIndex(Integer[] age) {
		
		int result[] = {0,0,0};
		int maxNum = Arrays.stream(age).mapToInt(x->x).max().orElse(-1); // 최대값
		List<Integer> ageList = Arrays.asList(age);	
		int count = (int) ageList.stream().filter(x->x==maxNum).count(); // 최대값 개수
		int ageIndex = ageList.indexOf(maxNum); //최대값 인덱스
		
		
		switch (count){
			case 1 :  
				result[ageIndex] = 1;
				break;
			case 2 :  
				for(int i=1; i<3; i++) {
					result[ageIndex] = i;
					age[ageIndex] = 0; 
					ageIndex = ageList.indexOf(maxNum);
				}
				break;
			case 3 : 				
				for(int i=1; i<4; i++) {
					result[ageIndex] = i;
					age[ageIndex] = 0; 
					ageIndex = ageList.indexOf(maxNum);
				} 
				break;
			default : break;
		}
						
		return result;
	}
	
	
}
