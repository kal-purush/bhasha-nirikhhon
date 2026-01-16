import java.util.ArrayList;

public class Cell {
	
	public int col;		// col coordinate
	public int row;		// row coordinate
	public int val;		// cell value
	public ArrayList<Integer> domain;
	

	/**
	 * Constructor
	 */
	public Cell(int col, int row)	{
		this.col = col;
		this.row = row;
		val = 0;
		initDomain();
	}
	
	public Cell(int col, int row, int val)	{
		this.col = col;
		this.row = row;
		this.val = val;
		initDomain();
	}
	
	
	/**
	 * Initialize the domain, 1 to 9 included
	 */
	public void initDomain()	{
		for (int i = 1; i <= 9; i++)
			this.domain.add(i);
	}
	
	
	/**
	 * Setter for the value variable
	 * @param val
	 */
	public void setValue(int val)	{
		this.val = val;
	}
	
	
	/**
	 * Getter for the domain variable
	 * @return
	 */
	public ArrayList<Integer> getDomain()	{
		return this.domain;
	}
	
	
	/**
	 * Setter for the domain variable
	 * @param domain
	 */
	public void setDomain(ArrayList<Integer> domain)	{
		this.domain = domain;
	}
}