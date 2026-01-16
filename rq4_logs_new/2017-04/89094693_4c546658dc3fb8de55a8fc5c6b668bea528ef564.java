/* Jessica Gary
 * jsg2213
 * COMS3134 Columbia 2017
 * 
 * HW3 Programming part 2 
 * 
 * ************Modified the following methods/class:***************
 * AvlNode class - to accept linked_list & line numbers, each Node should contain a unique word and a linked list of where that word occurs in the text
 * insert 		 - to accept line numbers 
 * 
 * ******************Added the following methods:******************
 * public void indexWord(String word, int line) -->  adds an occurrence of the word word in line line
 * public List getLinesForWord(String word)     -->  looks up a word and returns a list of lines in which it occurs.
 * public void printIndex()						-->  prints out each unique word that is stored in the Avl tree along with a list of line numbers in which that word appears
 * 	
 */
// AvlTree class
// CONSTRUCTION: with no initializer
//
// ******************PUBLIC OPERATIONS*****************************
// void insert( x )       --> Insert x
// void remove( x )       --> Remove x (unimplemented)
// boolean contains( x )  --> Return true if x is present
// AvlNode<String> find( String x ) --> Return the AvlNode if x is present in tree
// boolean remove( x )    --> Return true if x was present
// Comparable findMin( )  --> Return smallest item
// Comparable findMax( )  --> Return largest item
// boolean isEmpty( )     --> Return true if empty; else false
// void makeEmpty( )      --> Remove all items
// void printTree( )      --> Print tree in sorted order
// ******************ERRORS****************************************
// Throws UnderflowException as appropriate

/**
 * Implements an AVL tree.
 * Note that all "matching" is based on the compareTo method.
 * @author Mark Allen Weiss
 * 
 */
import java.util.*; 
public class AvlTree<AnyType extends Comparable<? super AnyType>>
{
    /**
     * Construct the tree.
     */
    public AvlTree( )
    {
        root = null;
    }

    /**
     * Insert into the tree; duplicates are ignored.
     * @param x the item to insert.
     */
    public void insert( String x, int line_num )
    {
       root = insert( x, line_num, root );
    }

    /**
     * Remove from the tree. Nothing is done if x is not found.
     * @param x the item to remove.
     */
    public void remove( String x )
    {
        root = remove( x, root );
    }

       
    /**
     * Internal method to remove from a subtree.
     * @param x the item to remove.
     * @param t the node that roots the subtree.
     * @return the new root of the subtree.
     */
    private AvlNode<String> remove( String x, AvlNode<String> t )
    {
        if( t == null )
            return t;   // Item not found; do nothing
            
        int compareResult = x.compareTo( t.element );
            
        if( compareResult < 0 )
            t.left = remove( x, t.left );
        else if( compareResult > 0 )
            t.right = remove( x, t.right );
        else if( t.left != null && t.right != null ) // Two children
        {
            t.element = findMin( t.right ).element;
            t.right = remove( t.element, t.right );
        }
        else
            t = ( t.left != null ) ? t.left : t.right;
        return balance( t );
    }
    
    /**
     * Find the smallest item in the tree.
     * @return smallest item or null if empty.
     */
    public String findMin( )
    {
        if( isEmpty( ) )
            throw new UnderflowException( );
        return findMin( root ).element;
    }

    /**
     * Find the largest item in the tree.
     * @return the largest item of null if empty.
     */
    public String findMax( )
    {
        if( isEmpty( ) )
            throw new UnderflowException( );
        return findMax( root ).element;
    }

    /**
     * Find an item in the tree.
     * @param x the item to search for.
     * @return true if x is found.
     */
    public boolean contains( String x )
    {
        return contains( x, root );
    }

    public AvlNode<String> find( String x )
    {
    	AvlNode<String> temp = find(x, root); 
    	
        return temp; 
    }
    
    /**
     * Make the tree logically empty.
     */
    public void makeEmpty( )
    {
        root = null;
    }

    /**
     * Test if the tree is logically empty.
     * @return true if empty, false otherwise.
     */
    public boolean isEmpty( )
    {
        return root == null;
    }

    /**
     * Print the tree contents in sorted order.
     */
    public void printTree( )
    {
        if( isEmpty( ) )
            System.out.println( "Empty tree" );
        else
            printTree( root );
    }

    private static final int ALLOWED_IMBALANCE = 1;
    
    // Assume t is either balanced or within one of being balanced
    private AvlNode<String> balance( AvlNode<String> t )
    {
        if( t == null )
            return t;
        
        if( height( t.left ) - height( t.right ) > ALLOWED_IMBALANCE )
            if( height( t.left.left ) >= height( t.left.right ) )
                t = rotateWithLeftChild( t );
            else
                t = doubleWithLeftChild( t );
        else
        if( height( t.right ) - height( t.left ) > ALLOWED_IMBALANCE )
            if( height( t.right.right ) >= height( t.right.left ) )
                t = rotateWithRightChild( t );
            else
                t = doubleWithRightChild( t );

        t.height = Math.max( height( t.left ), height( t.right ) ) + 1;
        return t;
    }
    
    public void checkBalance( )
    {
        checkBalance( root );
    }
    
    private int checkBalance( AvlNode<String> t )
    {
        if( t == null )
            return -1;
        
        if( t != null )
        {
            int hl = checkBalance( t.left );
            int hr = checkBalance( t.right );
            if( Math.abs( height( t.left ) - height( t.right ) ) > 1 ||
                    height( t.left ) != hl || height( t.right ) != hr )
                System.out.println( "OOPS!!" );
        }
        
        return height( t );
    }
    
    
    /**
     * Internal method to insert into a subtree.
     * @param x the item to insert.
     * @param t the node that roots the subtree.
     * @return the new root of the subtree.
     */
    private AvlNode<String> insert( String x, int line_num, AvlNode<String> t )
    {
        if( t == null ) //empty tree
            return new AvlNode<String>( x, line_num, null, null ); 
        
        //duplicates are NOT ignored 
        if(this.contains(x))
        	this.indexWord(x, line_num); //add the occurrence of the word x if this word already exists in the tree 
        else 
        {
        	//if the word doesn't already exist in the tree, then insert the node in appropriate place: 
        	int compareResult = x.compareTo( t.element );
            
            if( compareResult < 0 )
                t.left = insert( x, line_num, t.left );
            else if( compareResult > 0 )
                t.right = insert( x, line_num, t.right );
            else
                ;  // Duplicate; do nothing
            return balance( t );
        }
       return t; 
        
    }

    /**
     * Internal method to find the smallest item in a subtree.
     * @param t the node that roots the tree.
     * @return node containing the smallest item.
     */
    private AvlNode<String> findMin( AvlNode<String> t )
    {
        if( t == null )
            return t;

        while( t.left != null )
            t = t.left;
        return t;
    }

    /**
     * Internal method to find the largest item in a subtree.
     * @param t the node that roots the tree.
     * @return node containing the largest item.
     */
    private AvlNode<String> findMax( AvlNode<String> t )
    {
        if( t == null )
            return t;

        while( t.right != null )
            t = t.right;
        return t;
    }

    /**
     * Internal method to find an item in a subtree.
     * @param x is item to search for.
     * @param t the node that roots the tree.
     * @return true if x is found in subtree.
     */
    private boolean contains( String x, AvlNode<String> t )
    {
        while( t != null )
        {
            int compareResult = x.compareTo( t.element );
            
            if( compareResult < 0 )
                t = t.left;
            else if( compareResult > 0 )
                t = t.right;
            else
                return true;    // Match
        }

        return false;   // No match
    }

    /**
     * Internal method to print a subtree in sorted order.
     * @param t the node that roots the tree.
     */
    private void printTree( AvlNode<String> t )
    {
        if( t != null )
        {
            printTree( t.left );
            System.out.println( t.element );
            printTree( t.right );
        }
    }

    /**
     * Return the height of node t, or -1, if null.
     */
    private int height( AvlNode<String> t )
    {
        return t == null ? -1 : t.height;
    }

    /**
     * Rotate binary tree node with left child.
     * For AVL trees, this is a single rotation for case 1.
     * Update heights, then return new root.
     */
    private AvlNode<String> rotateWithLeftChild( AvlNode<String> k2 )
    {
        AvlNode<String> k1 = k2.left;
        k2.left = k1.right;
        k1.right = k2;
        k2.height = Math.max( height( k2.left ), height( k2.right ) ) + 1;
        k1.height = Math.max( height( k1.left ), k2.height ) + 1;
        return k1;
    }

    /**
     * Rotate binary tree node with right child.
     * For AVL trees, this is a single rotation for case 4.
     * Update heights, then return new root.
     */
    private AvlNode<String> rotateWithRightChild( AvlNode<String> k1 )
    {
        AvlNode<String> k2 = k1.right;
        k1.right = k2.left;
        k2.left = k1;
        k1.height = Math.max( height( k1.left ), height( k1.right ) ) + 1;
        k2.height = Math.max( height( k2.right ), k1.height ) + 1;
        return k2;
    }

    /**
     * Double rotate binary tree node: first left child
     * with its right child; then node k3 with new left child.
     * For AVL trees, this is a double rotation for case 2.
     * Update heights, then return new root.
     */
    private AvlNode<String> doubleWithLeftChild( AvlNode<String> k3 )
    {
        k3.left = rotateWithRightChild( k3.left );
        return rotateWithLeftChild( k3 );
    }

    /**
     * Double rotate binary tree node: first right child
     * with its left child; then node k1 with new right child.
     * For AVL trees, this is a double rotation for case 3.
     * Update heights, then return new root.
     */
    private AvlNode<String> doubleWithRightChild( AvlNode<String> k1 )
    {
        k1.right = rotateWithLeftChild( k1.right );
        return rotateWithRightChild( k1 );
    }
    public String getElement(AvlNode<String> node)
    {
    	return node.element; 
    }
    private static class AvlNode<String>
    {
            // Constructors
        AvlNode( String theElement, int line_num )
        {
            this( theElement, line_num, null, null );
        }

        AvlNode( String theElement, int line_num, AvlNode<String> lt, AvlNode<String> rt )
        {
        	//System.out.println("Just inserted a new Node: "+ theElement+" ---at line " + line_num);
            element  = theElement; //the word 
            line_list.add(line_num); //the line number -- we know it is the first occurance of the word 
            left     = lt;
            right    = rt;
            height   = 0;
        }

        //Each element of the AVL tree (NODE) should contain a unique word and a linked list of line numbers where that word occurs. 
        String           element;      // The data in the node = the unique word 
        LinkedList<Integer> line_list = new LinkedList<Integer>(); //a linked list of line numbers where that word occurs. 
        AvlNode<String>  left;         // Left child
        AvlNode<String>  right;        // Right child
        int               height;       // Height
    }
  
    private AvlNode<String> find( String x, AvlNode<String> t )
    { 
    	//author: Weiss 
        while( t != null )
            if( x.compareTo( t.element ) < 0 )
                t = t.left;
            else if( x.compareTo( t.element ) > 0 )
                t = t.right;
            else
                return t;    // Match

        return null;   // No match
    }

    public void indexWord(String word, int line) 
    {
    	/*
    	 *  adds an occurrence of the word word in line line. 
    	 *  If a word already exists in the AVL Tree, simply add the new line number to the existing node. 
    	 *  If a word appears on the same line twice, it should only have one entry in the list for that line.
    	 */
    	
    	if(this.contains(word)) //if the tree already contains the word
    	{
    		//contains() returns true if the word already exists in the AVL tree
    		//add the new line number to the existing node LinkedList 
    		AvlNode<String> tempNode = find(word); //find that node that contains the word 
    		if(!tempNode.line_list.contains(line)) //check if the line # already appears 
    		{
    			//if the line number doesn't exist in the linked list already -->> add it.  
    			tempNode.line_list.add(line); //add line number to the linked list in the node --> word already exists 
    		}
    		
    		// Note: a better way to do this without calling contains AND find? --> order n^2? 
    	}
    }
   
    public LinkedList<Integer> getLinesForWord(String word)
    {
    	//looks up a word and returns a list of lines in which it occurs.
    	
    	if(this.contains(word))
    	{
    		AvlNode<String> tempNode = find(word); //find the word in the AVL tree
    		return tempNode.line_list; 
    	}
    	else
    	{
    		System.out.println("This word cannot be found in the file (AVL tree).");
    		return null; 
    	}
    }
    
    public void printIndex()
    {
    	//calls a private function printIndex(root); 
    	printIndex(root);
 
    }
    private void printIndex(AvlNode<String> t)
    {
    	/*prints out each unique word that is stored in the Avl tree 
    	 * along with a list of line numbers in which that word appears.
  		*/
    	
    	if( t != null )
        {
            printIndex( t.left );
            System.out.print(t.element + " ---in lines: "); //print out the unique word 
            
            //get the line number from the linked list 
           
            for(int i=0; i<t.line_list.size(); i++)
            {
            	System.out.print(t.line_list.get(i)+" "); 
            }
            System.out.println(); 
            
            printIndex( t.right );
        }
    	
    }
    
    /** The tree root. */
    private AvlNode<String> root;

   
        // Test program
    /*
    public static void main( String [ ] args )
    {
        AvlTree<Integer> t = new AvlTree<Integer>( );
        final int SMALL = 40;
        final int NUMS = 1000000;  // must be even
        final int GAP  =   37;

        System.out.println( "Checking... (no more output means success)" );

        for( int i = GAP; i != 0; i = ( i + GAP ) % NUMS )
        {
        //    System.out.println( "INSERT: " + i );
            t.insert( i );
            if( NUMS < SMALL )
                t.checkBalance( );
        }
        
        for( int i = 1; i < NUMS; i+= 2 )
        {
         //   System.out.println( "REMOVE: " + i );
            t.remove( i );
            if( NUMS < SMALL )
                t.checkBalance( );
        }
        if( NUMS < SMALL )
            t.printTree( );
        if( t.findMin( ) != 2 || t.findMax( ) != NUMS - 2 )
            System.out.println( "FindMin or FindMax error!" );

        for( int i = 2; i < NUMS; i+=2 )
             if( !t.contains( i ) )
                 System.out.println( "Find error1!" );

        for( int i = 1; i < NUMS; i+=2 )
        {
            if( t.contains( i ) )
                System.out.println( "Find error2!" );
        }
    }
    */
}