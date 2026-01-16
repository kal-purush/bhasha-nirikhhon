import java.util.ArrayList;

public class GeneratePermutations {
	
	public static void main(String args[]) {
		GeneratePermutations g = new GeneratePermutations();
		String[] elements = { "9", "3", "1"};
		ArrayList<String> permutations = g.generatePermutations(elements);
		for (String s : permutations) {
			System.out.println(s);
		}
	}

	private ArrayList<String> generatePermutations(String[] elements) {
		ArrayList<String> permutations = new ArrayList<String>();
		if (elements.length == 2) {

			String x1 = elements[0] + elements[1];
			String x2 = elements[1] + elements[0];
			permutations.add(x1);
			permutations.add(x2);

		} else {
			for (int i = 0; i < elements.length; i++) {
				String[] elements2 = new String[elements.length - 1];
				int kalo = 0;
				for (int j = 0; j < elements2.length; j++) {
					if (i == j) {
						kalo = 1;
					}
					elements2[j] = elements[j + kalo];
				}
				ArrayList<String> k2 = generatePermutations(elements2);
				for (String x : k2) {
					String s = elements[i] + x;
					permutations.add(s);
				}
			}
		}

		return permutations;
	}
}