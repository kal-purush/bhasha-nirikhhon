package pl.frati.dynx.tasks;

/**
 * <p>
 * Interface for works which progression may be measured.
 * </p>
 * 
 * @author Rafal Figas
 *
 */
public interface Progression {

	/**
	 * <p>
	 * Gets current progress.
	 * </p>
	 * 
	 * @return Value from 0 to 1 (both inclusive) meaning progression of work. 0
	 *         means that no progress has been done yet, 1 means that all the
	 *         work is now complete.
	 */
	double getProgress();
}