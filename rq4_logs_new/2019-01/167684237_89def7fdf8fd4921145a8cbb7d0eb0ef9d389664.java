

	
	import java.util.ArrayList;
	import java.util.Arrays;
	import java.util.HashSet;
	import java.util.List;
	import java.util.Set;

	public class Main {

		public static void main(String[] args) {
			
			List<String> duplicates = Arrays.asList("Ivan", "Piotr", "Ivan", "Piotr", "Anna");
			
			System.out.println(duplicates);
			System.out.println(removeDuplicates(duplicates));
			
			

		}
		
		public static List<String> removeDuplicates(List<String> list){
			
			Set<String> set = new HashSet<>(list);
			
			
			return new ArrayList<>(set);
		}
		
		
	}

