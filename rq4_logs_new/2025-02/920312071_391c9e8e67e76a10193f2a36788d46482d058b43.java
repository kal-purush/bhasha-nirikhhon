import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
    	Scanner sc = new Scanner(System.in);
    		
    	int N = sc.nextInt();
    	int[] trees = new int[N];
    	int sum_tree = 0;
    	
    	List<Integer> zero = new ArrayList<>();
    	List<Integer> one = new ArrayList<>();
    	List<Integer> two = new ArrayList<>();
    	List<Integer> three = new ArrayList<>();
    	
    	for(int i = 0; i < N; i++) {
    		trees[i] = sc.nextInt();
    		sum_tree += trees[i];
    	}
    	
    	Arrays.sort(trees);
    	
    	for(int i = 0; i < N; i++) {
    		if(trees[i] % 3 == 1) {
				one.add(i);
			}
    		if(trees[i] % 3 == 2) {
    			two.add(i);
    		}
    		if(trees[i] % 3 == 0) {
    			three.add(i);
    		}
    	}
    	
    	if(sum_tree % 3 != 0) {
    		System.out.println("NO");
    	}
    	
    	else {
			int water = sum_tree / 3;
			int index = 0;
			
			while(water >= 0 && !one.isEmpty()) {
				index = one.remove(0);
				trees[index] -= 1;
				if(trees[index] == 0)
					zero.add(index);
				else
					three.add(index);
				
				if(two.isEmpty()) {
					index = one.remove(one.size() - 1);
					if(trees[index] == 1) {
						one.add(index);
						if(three.isEmpty()) {
							break;
						}
						else {
							index = three.remove(three.size() - 1);
							trees[index] -= 2;
							one.add(index);
							water -= 1;
						}
					}
					else {
						trees[index] -= 2;
						two.add(index);
						water -= 1;
					}
				}
				
				else {
					index = two.remove(two.size() - 1);
					trees[index] -= 2;
					if(trees[index] == 0)
						zero.add(index);
					else
						three.add(index);
					water -= 1;
				}
			}	
						
			
			if(one.isEmpty())
				System.out.println("YES");
			else {
				System.out.println("NO");
			}
    	}
    }
}