import java.util.*;

public class Entropy {
	Hashtable entropy = new Hashtable();
	Enumeration key;
	String str;
	double entr;
	int[] counter;
	
	//Constructor for the hashtable and counter array. User sends their idArray over and both are built. 
	public Entropy(String[] clicks){
		for(int i = 0; i < clicks.length; i++){
			entropy.put(clicks[i], new Double(0.0));
		}
		
		counter = new int[clicks.length];
		for(int i = 0; i < clicks.length; i++){
			counter[i] = 0;
		}

	}
	
	//Counts through the array and records in the counter array the amount for each element. 
	public int[] count (String[] clicks){
		for(int i = 0; i < counter.length; i ++){
			counter[i] = 0;
		}
		key = entropy.keys();
		while(key.hasMoreElements()){
			str = (String) key.nextElement();
			int foo = Integer.parseInt(str) - 1;
			for(int i = 0; i<clicks.length; i++){
				if(str == clicks[i]){
					counter[foo]++;
				}
			}
		}
		return counter;
	}
	
	//Does the entropy calculation and stores it to the hashtable locations
	public void calcEntropy(String[] clickArray, int[] count){
		key = entropy.keys();
		while(key.hasMoreElements()){
			str = (String) key.nextElement();
			int foo = Integer.parseInt(str) - 1; 
			int total = clickArray.length;
			double prob = (double)count[foo]/(double)total;
			entr = ((Double)entropy.get(str)).doubleValue();
			if(count[foo] == 0){
				entropy.put(str, new Double(entr + 0));
			}
			else{
				entr = entr + -(prob * Math.log10(prob));
				entropy.put(str, new Double(entr));	
			}
		}
	}
	
	//Displays All Entropy Values 
	public void displayAll(){
		key = entropy.keys();
		while(key.hasMoreElements()){
			str = (String) key.nextElement();
			System.out.println(str + ": "+ entropy.get(str));
		}
		System.out.println();
		return;
	}
	
	//Displays A Specific Entropy Value
	public void displayOne(String choice){ 
		key = entropy.keys();
		while(key.hasMoreElements()){
			str = (String) key.nextElement();
			if(str == choice){
				System.out.println(str + ": " + entropy.get(str));
				return;
			}
		}
		System.out.println("Could not find "+ choice+".");
		return;
	}
}