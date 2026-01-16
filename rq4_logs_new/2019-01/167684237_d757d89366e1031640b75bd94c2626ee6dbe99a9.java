import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Main {

	public static void main(String[] args) {
		
		List<String> duplicates = Arrays.asList("Ivan", "Piotr", "Ivan", "Piotr", "Anna");
		
		System.out.println(duplicates);
		System.out.println(getDuplicates(duplicates));
		
		

	}
	
	public static List<String> getDuplicates(List<String> list){
		
		Set<String> set = new HashSet<>();
		List<String> doubles = new ArrayList<String>();
		for (String s : list) {
			if (!set.add(s))
			{doubles.add(s);};
		}
		
		
		return doubles;
	}
	
}