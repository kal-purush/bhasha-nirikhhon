package decaf.error;

import decaf.Location;

/**
 * example：incompatible operands: int + bool<br>
 * PA2
 */
public class IncompatTerOpError extends DecafError {

	private String left;

	private String right;

	public IncompatTerOpError(Location location, String left,
			String right) {
		super(location);
		this.left = left;
		this.right = right;
	}

	@Override
	protected String getErrMsg() {
		return "incompatible condition operates: " + left + " " + "and" + " " + right;
	}

}