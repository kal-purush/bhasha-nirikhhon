/**
 * 
 * class used to return both the index of the item found
 * 		and the number of iterations it took to find result
 *
 */
public class SearchResult {
	
	/**
	 * index of the item found - should be -1 if not found
	 */
	private int index;
	
	/**
	 * number of iterations in the search loop
	 */
	private int iterations;

	/**
	 * constructor to create a new search result
	 * @param index
	 * @param iterations
	 */
	public SearchResult(int index, int iterations) {
		this.index = index;
		this.iterations = iterations;
	}
	
	/**
	 * 
	 * @return index of item found
	 */
	public int getIndex() {
		return index;
	}
	
	/**
	 * 
	 * @return number of iterations in search loop
	 */
	public int getIterations() {
		return iterations;
	}
}