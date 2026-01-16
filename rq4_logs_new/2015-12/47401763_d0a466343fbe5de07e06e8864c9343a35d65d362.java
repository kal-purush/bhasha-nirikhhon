import java.util.ArrayList;
import java.util.List;

public class Test1 {

	public static void main(String[] args) {
		List<String> stringCollection = new ArrayList<>();
		stringCollection.add("ddd2");
		stringCollection.add("aaa2");
		stringCollection.add("bbb1");
		stringCollection.add("aaa1");
		stringCollection.add("bbb3");
		stringCollection.add("ccc");
		stringCollection.add("bbb2");
		stringCollection.add("ddd1");

		System.out.println(stringCollection.stream().allMatch(x -> x.contains("")));
		stringCollection.stream().filter(x -> x.contains("b")).forEach(x -> System.out.println(x));
	}
}