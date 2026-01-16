import java.util.Iterator;


public abstract class fingerTree <A,B> implements Iterable <b> 
{

    
    fingerTree() 
    {
    	
    }

  
    public abstract < C > C match(Matcher <A,B,C> matcher);

    /**
     * measure the tree
     * @return measure of the tree
     */
    public abstract A measure();

 
    public abstract fingerTree <A,B> addFirst(B b);

   
    public abstract fingerTree <A,B> addLast(B b);

  
    public abstract fingerTree <A,B> concat(fingerTree <A,B> other);

   
    public abstract boolean isEmpty();

    public abstract B first();
    public abstract B last();

    
    public abstract fingerTree <A,B> removeFirst();
    public abstract fingerTree<A,B> removeLast();

    //calculates the longest prefix of the tree for which is true//

    public abstract fingerTree <A,B> takeUntil(Predicate <A> predicate);
    public abstract fingerTree <A,B> dropUntil(Predicate <A> predicate);

    //calculates the longest prefix of the tree for which is true//
    public abstract fingerTree <A,B> takeUntil(Predicate <A> predicate, boolean inclusive);
    public abstract fingerTree <A,B> dropUntil(Predicate <A> predicate, boolean inclusive);

    protected abstract fingerTree <A,B> splitLeft(Predicate <A> predicate, A initial, boolean inclusive);
    protected abstract fingerTree <A,B> splitRight(Predicate <A> predicate, A initial, boolean inclusive);

    //splits tree into two halves, first half is longest prefix for which predicate.apply(tree.measure()) is true//

    public abstract Pair <fingerTree <A,B>, fingerTree <A,B> > split(Predicate <B> predicate);

    protected abstract Split <A,B> splitTree(Predicate <A> predicate, A initial);

    public abstract Iterator <B> iterator();

    public static abstract class Matcher <A,B,C> {
//return pair that has the prefix and suffix
        public C empty() 
        {
            return otherwise();
        }

        public C single(B b) 
        {
            return otherwise();
        }

        public C deep(Digit < A,B > prefix, fingerTree < A,Node <A,B> > middle, Digit <A,B> suffix) 
        {
            return otherwise();
        }

        public C otherwise() 
        {
            throw new UnsupportedOperationException();
        }
    }

	    private Supplier<fingerTree<A, B>> supplier;
	    private volatile fingerTree<A, B> tree;

	    public FingerTree(Supplier<fingerTree<a,b>> supplier) {
	        this.supplier = supplier;
	    }
// the supplier interface represents the function that doesn't take in the argument//
	    private fingerTree<A, B> getTree() {
	        fingerTree<A, B> localTree = tree;
	        if (localTree == null) {
	                if (tree == null) {
	                    localTree = supplier.get();
	                    tree = localTree;
	                    supplier = null;
	                } else {
	                    localTree = tree;
	                }
	            }
//returns local tree//
	        return localTree;
	    }
//searches for expressions in different locations//
	    public <C> C match(Matcher<A, B, C> matcher) {
	        return getTree().match(matcher);
	    }
//product of the number value//
	    public A measure() {
	        return getTree().measure();
	    }
//insert number at the beginning of a LinkedList//
	    public fingerTree<A, B> addFirst(B b) {
	        return getTree().addFirst(b);
	    }
//insert number at the end of the LinkedList
	    public fingerTree<A, B> addLast(B b) {
	        return getTree().addLast(b);
	    }
//returns the string with the value of the string passed into the method//
	    public fingerTree<A, B> concat(fingerTree<A, B> other) {
	        return getTree().concat(other);
	    }
//this checks if the string is empty or not and can either return as true or false depending on that//
	    public boolean isEmpty() {
	        return getTree().isEmpty();
	    }
//returns the first number of a tree set//
	    public B first() {
	        return getTree().first();
	    }
//returns the last number of the tree set//
	    public B last() {
	        return getTree().last();
	    }
//removes the first number from the LinkedList
	    public fingerTree<A, B> removeFirst() {
	        return getTree().removeFirst();
	    }
//removes the last number from the LinkedList//
	    public fingerTree<A, B> removeLast() {
	        return getTree().removeLast();
	    }
//the rest has been explained above and is really the same stuff//
	    public fingerTree<A, B> takeUntil(Predicate<A> predicate) {
	        return getTree().takeUntil(predicate);
	    }

	    public fingerTree<A, B> dropUntil(Predicate<A> predicate) {
	        return getTree().dropUntil(predicate);
	    }

	    public fingerTree<A, B> takeUntil(Predicate<A> predicate, boolean inclusive) {
	        return getTree().takeUntil(predicate, inclusive);
	    }

	    public fingerTree<A, B> dropUntil(Predicate<A> predicate, boolean inclusive) {
	        return getTree().dropUntil(predicate, inclusive);
	    }

	    public fingerTree<A, B> splitLeft(Predicate<A> predicate, A initial, boolean inclusive) {
	        return getTree().splitLeft(predicate, initial, inclusive);
	    }

	    public fingerTree<A, B> splitRight(Predicate<A> predicate, A initial, boolean inclusive) {
	        return getTree().splitRight(predicate, initial, inclusive);
	    }

	    public Pair<fingerTree<A, B>,FingerTree<A, B>> split(Predicate<A> predicate) {
	        return getTree().split(predicate);
	    }

	    public Split<A, B> splitTree(Predicate<A> predicate, A initial) {
	        return getTree().splitTree(predicate, initial);
	    }

	    public Iterator<B> iterator() {
	        return getTree().iterator();
	    }
	}
