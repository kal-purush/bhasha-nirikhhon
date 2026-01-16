package money;

public class Checking extends Investment {

	protected Checking(Double annualPercentageChange, Double principle) {
		super(annualPercentageChange, principle);
	}

	/**
	 * 
	 * @param principle
	 *            should be how much money is in the account
	 */
	public Checking(Double principle) {
		this(0.00, principle);
	}
}