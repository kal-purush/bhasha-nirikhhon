package de.tu_dortmund.ub.data;

/**
 * @author tgaengler
 */
public class TPUException extends Exception {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new TPU exception with the given exception message.
	 *
	 * @param exception the exception message
	 */
	public TPUException(final String exception) {

		super(exception);
	}

	/**
	 * Creates a new TPU exception with the given exception message
	 * and a cause.
	 *
	 * @param message the exception message
	 * @param cause   a previously thrown exception, causing this one
	 */
	public TPUException(final String message, final Throwable cause) {

		super(message, cause);
	}
}