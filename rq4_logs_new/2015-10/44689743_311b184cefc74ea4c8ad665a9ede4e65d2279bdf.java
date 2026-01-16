
/**
 * A factory class for generating JobWorkables to be placed in Jobs.
 * You can extend this class and then create a Submittor with an
 * instance of your extended class to create the "work" for Jobs.
 * @author matt
 * @version 1.0
 * <p>Copyright: Copyright (c) 2015, 2004 by Matt Evett</p>
 *
 */
public class WorkFactory {
	private int jobCount = 0;
	public JobWorkable createWork() 
	{
		System.out.println("TO_DO: complete WorkFactory.createWork");
		JobWorkableImplementation obj1 = new JobWorkableImplementation();
		/* 
		 * Return a new instance of a class that implements JobWorkable.
		 * You should provide that class.
		 */
		return obj1;
	}
}