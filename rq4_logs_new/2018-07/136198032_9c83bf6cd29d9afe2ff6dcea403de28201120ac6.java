package cashbox.com;

import java.io.*;
import java.util.*;


public class cashbox /*implements Runnable*/{
	public static void main(String args[]) {		 

		/* Should i use thread here?
		Thread cash  = new Thread();
		Thread cash1 = new Thread();
		Thread cash2 = new Thread();
		Thread cash3 = new Thread();
		Thread cash4 = new Thread();
		cash.start();cash1.start();cash2.start();cash3.start();cash4.start();*/
		
		ArrayList<Double> time = new ArrayList<Double>();
		time.add(0.5);time.add(1.0);time.add(1.5);time.add(2.0);
		time.add(2.5);time.add(3.0);time.add(3.5);time.add(4.0);
		time.add(4.5);time.add(5.0);time.add(5.5);time.add(6.0);
		time.add(6.5);time.add(7.0);time.add(7.5);time.add(8.0);
		
		try {
			//Scanner scanner = new Scanner(new File("src/cahbox/com/1.txt"));
			
		Scanner str  = new Scanner(new File("src/cashbox/com/1.txt"));
		Scanner str1 = new Scanner(new File("src/cashbox/com/2.txt"));
		Scanner str2 = new Scanner(new File("src/cashbox/com/3.txt"));
		Scanner str3 = new Scanner(new File("src/cashbox/com/4.txt"));
		Scanner str4 = new Scanner(new File("src/cashbox/com/5.txt"));
		
		ArrayList<Integer> nums  = new ArrayList<Integer>();
		ArrayList<Integer> nums1 = new ArrayList<Integer>();
		ArrayList<Integer> nums2 = new ArrayList<Integer>();
		ArrayList<Integer> nums3 = new ArrayList<Integer>();
		ArrayList<Integer> nums4 = new ArrayList<Integer>();		
		
		while (str.hasNextInt())
	    {
	        nums.add(str.nextInt());
	        nums1.add(str1.nextInt());
	        nums2.add(str2.nextInt());
	        nums3.add(str3.nextInt());
	        nums4.add(str4.nextInt());
	    }
		/*
		System.out.println(nums);
		System.out.println(nums1);
		System.out.println(nums2);
		System.out.println(nums3);
		System.out.println(nums4);*/

		int maxs = Collections.max(nums);
		int max = nums.indexOf(maxs);
		int maxs1 = Collections.max(nums1);
		int max1 = nums1.indexOf(maxs1);
		int maxs2 = Collections.max(nums2);
		int max2 = nums2.indexOf(maxs2);
		int maxs3 = Collections.max(nums3);
		int max3 = nums3.indexOf(maxs3);
		int maxs4 = Collections.max(nums4);
		int max4 = nums4.indexOf(maxs4);			
		
		/*double z = Math.max(max, max1);
		if (z<max2) z=max2;
		if (z<max3) z=max3;
		if (z<max4) z=max4;*/
		
					//if (((max2==max)&(max2==max1))|((max2==max3)&(max2==max4))) {
				//System.out.println(max2);//int = max2
				for (int i=0;i<time.size();i++) {
					if (i==Math.max(max4, max)) 
		            {System.out.println("Max people was at this hour - "+time.get(i));}					
				}//I know it's not quite working as it should be. Just had no time.( Will fix it.
			//}		
			/*
			System.out.println(maxs);
			System.out.println(max);
			System.out.println(maxs1);
			System.out.println(max1);
			System.out.println(maxs2);
			System.out.println(max2);
			System.out.println(maxs3);
			System.out.println(max3);
			*/		
			//cbox c = new cbox();
			
		}catch (Exception e){
			e.printStackTrace();
		}
	}
	/*@Override
	public void run() {
		
try {
	
}catch (Exception e) {
	e.printStackTrace();	
}
	}*/
}
/*
class cbox{
	//480 - time in min
	void check() {
	for (int i=0; i<480; i++) {
		for (int j=0; j<40; j++) {
			if (j==29) continue;
		}
	}
	}
}*/