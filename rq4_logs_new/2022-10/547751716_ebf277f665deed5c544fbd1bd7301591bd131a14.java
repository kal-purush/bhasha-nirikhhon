package data_structure_exp.exp1.que6system;

import java.util.Iterator;

public class MyArrayList<AnyTypet> implements Iterable<AnyTypet> {
	private static final int DEFAULT_CAPACITY = 10;
	private int theSize;
	private AnyTypet [ ] theItems;

	public MyArrayList( ) { 
		doClear( ); 
	}
 
	public void clear( ) { 
		doClear( ); 
	}
 
	private void doClear( ){ 
		theSize = 0; ensureCapacity( DEFAULT_CAPACITY ); 
	}
 
	public int size( ){ 
		return theSize; 
	}
	public boolean isEmpty( ){ 
		return size( ) == 0; 
	}
	public void trimToSize( ){ 
		ensureCapacity( size( ) ); 
	}
 
	public AnyTypet get( int idx ){
		if( idx < 0 || idx >= size( ) )
			throw new ArrayIndexOutOfBoundsException( );
		return theItems[ idx ];
	}
 
	public AnyTypet set( int idx, AnyTypet newVal ){
		if( idx < 0 || idx >= size( ) )
			throw new ArrayIndexOutOfBoundsException( );
		AnyTypet old = theItems[ idx ];
		theItems[ idx ] = newVal;
		return old;
	}
 
	public void ensureCapacity( int newCapacity ){
		if( newCapacity < theSize )
			return;
 
		AnyTypet [ ] old = theItems;
		theItems = (AnyTypet []) new Object[ newCapacity ];
		for( int i = 0; i < size( ); i++ )
			theItems[ i ] = old[ i ];
	}
  
	public boolean add( AnyTypet x ){
		add( size( ), x );
		return true;
    }   
    public void add( int idx, AnyTypet x ){
    	if( theItems.length == size( ) )
    		ensureCapacity( size( ) * 2 + 1 );
    	for( int i = theSize; i > idx; i-- )
    		theItems[ i ] = theItems[ i - 1 ];
    	theItems[ idx ] = x;   
    	theSize++;
    }
   
    public AnyTypet remove( int idx ){
    	AnyTypet removedItem = theItems[ idx ];
    	for( int i = idx; i < size( ) - 1; i++ )
    		theItems[ i ] = theItems[ i + 1 ];
   
    	theSize--;
    	return removedItem;
    }    
    
    public void printMyArraylist() {
    	Iterator<AnyTypet> it = iterator();
    	while(it.hasNext()) {
    		AnyTypet s = it.next();
			System.out.println(s);
		}    	
    }
      
    
    
    public java.util.Iterator<AnyTypet> iterator( ){ 
    	return new ArrayListIterator( ); 
    }
   
    private class ArrayListIterator implements java.util.Iterator<AnyTypet>{
    	private int current = 0;
   
    	public boolean hasNext( ){ 
    		return current < size( ); 
    	}
   
    	public AnyTypet next( ) {
    		if( !hasNext( ) )
    			throw new java.util.NoSuchElementException( );
    		return theItems[ current++ ];
    	}
   
    	public void remove( ){ 
    		MyArrayList.this.remove( --current ); 
    	}
    }    

 }