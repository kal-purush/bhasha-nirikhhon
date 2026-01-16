/**
 * An implementation of a stack that uses a {@List} as the underlying
 * data structure.
 * 
 * Not all operations on a stack will always be successful. For
 * example, a programmer may try to pop an element from an empty
 * stack. Since we hace not covered exceptions yet, we need another
 * mechanism to report errors. In order to do that, methods of this
 * list will return a {@see ReturnObject} that will contain either an
 * object or an error value of the right kind (as defined in {@see
 * ErrorMessage}).
 * 
 * @author PiJ
 */
public abstract class AbstractStack implements Stack {
    /**
     * The data structure where the elements on the stack are stored. 
     */
    protected List internalList;

    /**
     * Creates a new abstract stack using the provided list as the
     * underlying data structure.
     * 
     * Note: This constructor does not check whether the provided list
     * is null. Programmers must do their own checks. If a null list
     * is provided, a NullPointerException will be thrown at runtime
     * as soon as any operation is attempted on the underlying list.
     * 
     * @param list the list to be used 
     */
    public AbstractStack(List list) {
	  this.internalList = list;
    }
}
