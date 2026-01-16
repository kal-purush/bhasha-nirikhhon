package com.nku.collatz;

import java.util.Iterator;

/**
 * Given a particular seed the next operation will return back a the next number
 * in the Collatz Problem
 *
 *   <ul>
 *     <li><h3>Cases</h3></li>
 *     <li>n = n / 2 : when n is even</li>
 *     <li>n = 3 * n + 1 : when n is odd </li>
 *   </ul>
 *
 * @author Nick Meyer
 *
 */
public class CollatzNumberGenerator implements Iterator<Long>
{	
	/** Starting seed number */
	private long seed;
	
	/** the current number in the sequence */
	private long current;
	
	public CollatzNumberGenerator(long seed)
	{
		setSeed(seed);
	}
	
	@Override
	public boolean hasNext()
	{
		return current > 1;
	}

	@Override
	public Long next()
	{
		return current = (current % 2 == 0 ? current / 2 : 3 * current + 1); 
	}
	
	public void setSeed(long seed)
	{
		this.seed = seed;
		this.current = this.seed;
	}
	
	public long getSeed()
	{
		return this.seed;
	}
}