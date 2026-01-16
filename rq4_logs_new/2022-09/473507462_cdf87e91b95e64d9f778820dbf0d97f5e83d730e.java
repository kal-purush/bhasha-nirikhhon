package exceptions;

public class EmptyDestinationsQueueException extends Exception {
	
	private static final long serialVersionUID = 1L;

	public EmptyDestinationsQueueException() {
		this("");
	}
	
	public EmptyDestinationsQueueException(String error) {
		super(error);
	}
}