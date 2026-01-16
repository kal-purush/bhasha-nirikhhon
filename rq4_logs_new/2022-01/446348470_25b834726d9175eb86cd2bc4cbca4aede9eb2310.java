package Algorithm;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

public class PriorityQueue2 {

	public static void main(String[] args) {
		
		Integer[] arr = {4,5,10,24,6,7};
		
		// PriorityQueue<Integer> pq = new PriorityQueue<>(Collections.reverseOrder()); 내림차순
		PriorityQueue<Integer> pq = new PriorityQueue<>();
		
		//List<Integer> list = Arrays.asList(arr);
		
		pq.addAll(Arrays.asList(arr));
		
		while(!pq.isEmpty()) {
			System.out.println(pq.poll());
		}
		
			
		
	}

}