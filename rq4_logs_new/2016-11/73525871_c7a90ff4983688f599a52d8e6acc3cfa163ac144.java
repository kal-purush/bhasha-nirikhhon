package automaton;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Alphabet {

	private List<AlphabetItem> alphabetItems;
	private Map<String, Range> subRanges;

	public Alphabet(List<AlphabetItem> alphabetItems, Map<String, Range> subRanges) {
		this.alphabetItems = alphabetItems;
		this.subRanges = subRanges;

	}

	public Alphabet(String[] alphabetItems, Map<String, Range> subRanges) {
		this();
		addStringItems(Arrays.asList(alphabetItems));
		addSubRanges(subRanges);
	}

	public Alphabet(List<AlphabetItem> alphabetItems) {
		this(alphabetItems, new HashMap<>());
	}

	public Alphabet(String[] alphabetItems) {
		this(alphabetItems, new HashMap<>());
	}

	public Alphabet() {
		this(new ArrayList<>());
	}

	public void addAlphabetItem(AlphabetItem alphabetItem) {
		if (!containsAlphabetItem(alphabetItem))
			alphabetItems.add(alphabetItem);
	}

	public void addAlphabetItem(String alphabetItem) {
		addAlphabetItem(new AlphabetItem(alphabetItem));
	}

	public void addAlphabetItems(List<AlphabetItem> alphabetItems) {
		alphabetItems.forEach(this::addAlphabetItem);
	}

	public void addStringItems(List<String> alphabetItems) {
		alphabetItems.forEach(this::addAlphabetItem);
	}

	public boolean containsAlphabetItem(AlphabetItem alphabetItem) {
		return alphabetItems.contains(alphabetItem);
	}

	public boolean containsAlphabetItem(String alphabetItem) {
		return alphabetItems.contains(new AlphabetItem(alphabetItem));
	}

	public int indexOf(AlphabetItem alphabetItem) {
		return alphabetItems.indexOf(alphabetItem);
	}

	public int indexOf(String alphabetItem) {
		return indexOf(new AlphabetItem(alphabetItem));
	}

	public AlphabetItem getAlphabetItem(int index) {
		return alphabetItems.get(index);
	}

	public List<AlphabetItem> getAlphabetItems() {
		return alphabetItems;
	}

	public void addSubRange(String label, Range range) {
		if (!range.withinBounds(alphabetItems.size() - 1))
			throw new InvalidParameterException("endIndex of range exceeds alphabetItems");
		subRanges.put(label, range);
	}

	private void addSubRanges(Map<String, Range> subRanges) {
		subRanges.forEach(this::addSubRange);
	}

	public boolean containsSubRange(String label) {
		return subRanges.containsKey(label);
	}

	public Range getSubRange(String label) {
		return subRanges.get(label);
	}

	public List<AlphabetItem> getSubAlphabet(String rangeLabel) {
		Range r = getSubRange(rangeLabel);
		// if (r==null) return null;
		if (r == null)
			return new ArrayList<>();

		List<AlphabetItem> res = new ArrayList<>();
		for (int i = r.getStartIndex(); i <= r.getEndIndex(); i++) {
			res.add(getAlphabetItem(i));
		}
		return res;
	}

	public static List<String> getLowerLatinAlphabet() {
		return charListOfString("abcdefghijklmnopqrstuvwxyz");
	}

	public static String[] getLowerLatinAlphabetA() {
		return stringListToArray(charListOfString("abcdefghijklmnopqrstuvwxyz"));
	}

	public static List<String> getUpperLatinAlphabet() {
		return charListOfString("abcdefghijklmnopqrstuvwxyz".toUpperCase());
	}

	public static String[] getUpperLatinAlphabetA() {
		return stringListToArray(charListOfString("abcdefghijklmnopqrstuvwxyz".toUpperCase()));
	}

	public static List<String> getArabicNumerals() {
		return charListOfString("0123456789");
	}

	public static String[] getArabicNumeralsA() {
		return stringListToArray(charListOfString("0123456789"));
	}

	private static String[] stringListToArray(List<String> list) {
		return list.toArray(new String[0]);
	}

	private static List<String> charListOfString(String string) {
		List<String> res = new ArrayList<String>();
		for (Character charT : string.toCharArray()) {
			res.add(charT.toString());
		}
		return res;
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

		Alphabet a = new Alphabet(getLowerLatinAlphabetA());

		a.addSubRange("l_t", new Range(a.indexOf("l"), a.indexOf("t")));
		a.addSubRange("w_y", new Range(a.indexOf("w"), a.indexOf("y")));

		System.out.println(a.getSubAlphabet("l_t"));
		System.out.println(a.getSubAlphabet("w_y"));
		System.out.println(a.getAlphabetItems());
	}
}