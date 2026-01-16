import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Alphabet {

	private List<Character> alphabetItems;
	private Map<String, Range> subRanges;

	public Alphabet(List<Character> alphabetItems, Map<String, Range> subRanges) {
		this.alphabetItems = alphabetItems;
		this.subRanges = subRanges;

	}

	public Alphabet(List<Character> alphabetItems) {
		this(alphabetItems, new HashMap<>());
	}

	public Alphabet() {
		this(new ArrayList<>());
	}

	public void addAlphabetItem(char alphabetItem) {
		if (!containsAlphabetItem(alphabetItem))
			alphabetItems.add(alphabetItem);
	}

	public boolean containsAlphabetItem(char alphabetItem) {
		return alphabetItems.contains(alphabetItem);
	}

	public int indexOf(char alphabetItem) {
		return alphabetItems.indexOf(alphabetItem);
	}

	public Character getAlphabetItem(int index) {
		return alphabetItems.get(index);
	}

	public List<Character> getAlphabetItems() {
		return alphabetItems;
	}

	public void addSubRange(String label, Range range) {
		if (!range.withinBounds(alphabetItems.size() - 1))
			throw new InvalidParameterException("endIndex of range exceeds alphabetItems");
		subRanges.put(label, range);
	}

	public Range getSubRange(String label) {
		return subRanges.get(label);
	}

	public List<Character> getSubAlphabet(String rangeLabel) {
		Range r = getSubRange(rangeLabel);
		// if (r==null) return null;
		if (r == null)
			return new ArrayList<>();

		List<Character> res = new ArrayList<>();
		for (int i = r.getStartIndex(); i <= r.getEndIndex(); i++) {
			res.add(getAlphabetItem(i));
		}
		return res;
	}

	public static List<Character> getLowerLatinAlphabet() {
		List<Character> res = new ArrayList<>();
		for (Character charT : "abcdefghijklmnopqrstuvwxyz".toCharArray()) {
			res.add(charT);
		}
		return res;
	}

	private static class Range {
		private int startIndex;
		private int endIndex;

		public Range(int startIndex, int endIndex) {
			if (startIndex > endIndex)
				throw new InvalidParameterException("startIndex should not exceed endIndex");
			this.startIndex = startIndex;
			this.endIndex = endIndex;
		}

		public boolean withinBounds(int maxIndex) {
			return endIndex <= maxIndex;
		}

		public int getStartIndex() {
			return startIndex;
		}

		public int getEndIndex() {
			return endIndex;
		}

	}

	public static void main(String[] args) {
		/*
		 * Alphabet a = new Alphabet();
		 * 
		 * a.addAlphabetItem("test1", Arrays.asList('1', '2'));
		 * System.out.println(a.containsLabel("test1"));
		 * a.addAlphabetItem("test2", Arrays.asList('3', '4'));
		 * 
		 * Map<String, Integer> map = new HashMap<>();
		 * 
		 * map.put("bla1", 2); map.put("bla2", 3); map.put("bla1", 4);
		 * 
		 * System.out.println(map.size() + " " + map.get("bla1"));
		 */

		Alphabet a = new Alphabet(getLowerLatinAlphabet());

		a.addSubRange("l_t", new Range(a.indexOf('l'), a.indexOf('t')));
		a.addSubRange("w_y", new Range(a.indexOf('w'), a.indexOf('y')));

		System.out.println(a.getSubAlphabet("l_t"));
		System.out.println(a.getSubAlphabet("w_y"));
		System.out.println(a.getAlphabetItems());
	}
}