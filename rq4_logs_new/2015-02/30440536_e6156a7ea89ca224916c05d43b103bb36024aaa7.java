package nQueenProblem;

import java.util.Random;

/**
 * An <code>Individual</code> (a chromosome) is a candidate solution for a given
 * problem. Its representation depends on the specific problem to be solved. Two
 * individuals can be combined (see method crossover) to produce a new
 * offspring. As with natural chromosomes, these artificial ones suffer
 * mutations. Each chromosome has a fitness value that indicates the quality of
 * this solution.
 * <p/>
 * 
 * A <code>Population</code> is a collection of chromosomes. At each iteration
 * (generation), the genetic algorithm selects chromosomes for reproduction. The
 * offsprings are inserted into the population, and the least fitted individuals
 * are eliminated. Thus, the size of the population is fixed.
 * <p/>
 * 
 * For this assignment, an <code>Individual</code> represents a solution to the
 * <code>n</code>-Queens problem. As introduced in the assignment description, a
 * candidate solution is represented by a permutation of size <code>n</code>,
 * such that attribute <code>i</code> represents the row for the queen found at
 * column <code>i</code>.
 * <p/>
 * 
 * Not all permutations are valid solutions to <code>n</code>-Queens problem. A
 * permutation is a valid solution if no two queens can attack each other. Two
 * queens are attacking each other if they are on the same row or column, which
 * is impossible given this representation, but also if they are found on the
 * same minor or major diagonal.
 * <p/>
 *
 * Herein, we define the fitness value of an individual as the number of pairs
 * of queens attacking each other.
 * <p/>
 * You must complete the implementation of the class <code>Individual</code>
 * following all the directives.
 *
 * @author Marcel Turcotte (turcotte@eecs.uottawa.ca)
 */


public class Individual implements Comparable<Individual> {
private int fitness=0;
private int [] permutation;
private final int size;
    
public Individual(int[] permutation) { // 10
this.permutation=permutation;
size=permutation.length;
for (int i=0; i<size; i++)
{
	 for (int j=0; j<size; j++)
	 {
		 /*
		  * i represents the column of the queen currently being looked at
		  * j represents the "position", which is column being looked at.
		  * permutation[j] is the queen in the column being looked at
		  * permutation[i] is the row of the queen being looked at.
		  * i-j represents the horizontal distance between columns
		  * permutation i-permutation j is the vertical distance between the rows.
		  * if the absolute value of the vertical distance between 2 queens
		  * is equal to the absolute value of the horizontal distance between queens
		  * then the the queens are diagonal to each other.
		  * The code test if the queen at (i,permutation[i]) is diagonal to the queen at (j, permutation[j])
		  * if so the queens are attacking each other so the fitness score goes up.
		  * Due to the way permutations are defined, this is the only way a permutation can allow 2 queens to attack each other.
		  */
		 if ((permutation[j]==permutation[i]+j-1)||(permutation[j]==permutation[i]-j+1))
fitness++;
	 }
}
/*
 *  We are counting queen A attacking queen B and queen B attacking queen a as one attack, so it 
 *  only increase the fitness score by 1 for every 2 queens threatening each other.
 */
fitness=fitness/2;
	// REPLACE THE BODY OF THIS METHOD WITH YOUR OWN IMPLEMENTATION

    }
    public Individual(int size) {
 permutation=Util.getPermutation(size);
 //Gets the fitness using the
Individual test=new Individual(permutation);
fitness=test.fitness;
this.size=size;
 
	// REPLACE THE BODY OF THIS METHOD WITH YOUR OWN IMPLEMENTATION

    }

    /**
     * Creates an <code>Individual</code> using the provided permutation. The method
     * must copy the values of the permutation into a new array. This constructor
     * is primarily used for testing.
     * 
     * @param permutation used to initialize the attributes of this <code>Individual</code>
     */

    

    /**
     * Returns the offspring resulting from the crossover of <code>this</code>
     * <code>Individual</code> and <code>other</code>. The result must be a valid
     * permutation!
     * <p/>
     * 
     * In particular, the naive solution consisting of taking the first
     * <code>position-1</code> attributes of this <code>Individual</code> and
     * the last <code>size-position</code> attributes of <code>other</code> would
     * not generate a valid permutation in most cases.
     * <p/>
     *  
     * Instead, we are proposing that the first <code>position-1</code> attributes 
     * of this <code>Individual</code> are copied to the offspring, then the
     * missing values will be selected from <code>other</code>, whilst preserving
     * their order of appearance in <code>other</code>.
     * <p/>
     * 
     * This method is primarily used for testing.
     * 
     * @param other a reference to an <code>Individual</code>
     * @param position the location of the crossover
     * @return the offspring resulting from the crossover of <code>this</code> and <code>other</code>
     */
    /*
     * first step is to copy the values before the position into a new permutation.
     * Second step Use the position as a starting point to add more values to the new permutation.
     * Third step compare all values in the original permutation to each value in the other permutation.
     * Fourth step if there is a match, move on to the next other permutation value to test.
     * Fifth step If no match found after comparing all values in the original permutation, add the other value 
     * the new permutation and increase the starting point to the next value in the new matrix (add 1)/.
     */
    public Individual crossover(Individual other, int position) {
    	//Step 1
    	int [] newPermutation= new int [other.size];
    	for (int i=0; i<position;i++)
    		newPermutation[i]=this.permutation[i];
    	//Step 2 (j is the starting point to add new values
    	int j=position;
    
    	while (j<=size)
    	{
    		//Step 3
    		for (int scrollOther=0; scrollOther<other.size;scrollOther++)
    		{
    			for (int scrollThis=0; scrollThis<position;scrollThis++)
    			{
    				
    				if ((permutation[scrollThis])==(permutation[scrollOther]))
    					//Step 4
    				scrollThis=position;
    				else if (scrollThis==position-1)
    				{
    					//Step 5
    			   newPermutation[j]=other.permutation[scrollOther];
    			   j++;
    				}
    			}
    		}
    	}
	Individual child = new Individual(newPermutation);
	return child;
	// REPLACE THE BODY OF THIS METHOD WITH YOUR OWN IMPLEMENTATION

    }

    /**
     * Returns the offspring resulting from the crossover of <code>this</code>
     * <code>Individual</code> and <code>other</code>. The method randomly selects the
     * position of the crossover. The result must be a valid permutation!
     * <p/>
     * 
     * In particular, the naive solution consisting of taking the first
     * <code>position-1</code> attributes of this <code>Individual</code> and the last
     * <code>size-position</code> attributes of <code>other</code> would not generate a
     * valid permutation in most cases.
     * <p/>
     * 
     * Instead, we are proposing that the first <code>position-1</code> attributes
     * of this <code>Individual</code> are copied to the offspring, then the missing
     * values will be selected from <code>other</code>, whilst preserving their
     * order of appearance in <code>other</code>.
     * <p/>
     * 
     * This method is used by <code>Population</code>.
     * 
     * @param other a reference to an <code>Individual</code>
     * @return the offspring resulting from the crossover of <code>this</code> and <code>other</code>
     */
    /*
     * Randomizes the position of the cross over point to anywhere along the permutation.
     */
    public Individual recombine(Individual other) {
    	
	int position= (int) Math.floor(Math.random()*size);
	return crossover( other, position);

    }
    
    /**
	 * Returns the offspring resulting from the crossover of <code>this</code>
	 * <code>Individual</code> and <code>other</code>. The method randomly selects the
	 * position of the crossover. The result must be a valid permutation!
	 * <p/>
	 * 
	 * In particular, the naive solution consisting of taking the first
	 * <code>position-1</code> attributes of this <code>Individual</code> and the last
	 * <code>size-position</code> attributes of <code>other</code> would not generate a
	 * valid permutation in most cases.
	 * <p/>
	 * 
	 * Instead, we are proposing that the first <code>position-1</code> attributes
	 * of this <code>Individual</code> are copied to the offspring, then the missing
	 * values will be selected from <code>other</code>, whilst preserving their
	 * order of appearance in <code>other</code>.
	 * <p/>
	 * 
	 * This method is used by <code>Population</code>.
	 * 
	 * @param other a reference to an <code>Individual</code>
	 * @return the offspring resulting from the crossover of <code>this</code> and <code>other</code>
	 */
	
	
    public Individual mutate(int i, int j) { 
    int newJ=this.permutation[i];
    int newI=this.permutation[j];
    int [] mutatedPermutation=new int [size];
    for (int x=0;x<size;x++)
    {
    	mutatedPermutation[x]=this.permutation[x];
    }
    mutatedPermutation[i]=newI;
    mutatedPermutation[j]=newJ;
    return new Individual (mutatedPermutation);
    }

    /**
     * Returns the offspring resulting from applying a mutation
     * to this <code>Individual</code>. In order to make sure that 
     * the result is valid permutation, the method exchanges
     * the value of two randomly selected attributes.
     * <p/>
     * 
     * This method is called by <code>Population</code>.
     * 
     * @return the offspring resulting from exchanging two randomly selected attributes
     */

    public Individual mutate() {
    	if (size==1)
    		return this;
    int i=(int) Math.floor(size*Math.random());
    int j=1;
    while (j==i)
    {
    j=(int) Math.floor(size*Math.random());
    }
    
	return this.mutate(i,j);
    }
    
    /**
     * Returns the fitness value of <code>this Individual</code>, which
     * is defined as the number of pairs of queens attacking each
     * other.
     * 
     * @return the fitness value of <code>this Individual</code>. 
     */

    public int getFitness() {

	return this.fitness;
    }

    
    /**
     * Returns a negative integer, zero, or a positive integer as the fitness of this <code>Individual</code> is
     * less than, equal to, or greater than the fitness of the specified <code>Individual</code>. 
     * 
     * @param other <code>Individual</code> to be compared
     * @return a negative integer, zero, or a positive integer as the fitness of this <code>Individual</code> 
     *         is less than, equal to, or greater than the fitness of the specified <code>Individual</code>.
     */
    
    public int compareTo(Individual other) {

	return (this.fitness-other.fitness);
    }

   
    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     * This creates a string of the permutation. A permutation of a b c d e
     * becomes {a,b,c,d,e}
     */
    private String permutationToString () {
    	

    	StringBuilder writePermutation = new StringBuilder();
    	writePermutation.append('{');
    	for (int i=0; i<size; i++)
    	{
    		writePermutation.append(this.permutation[i]);
    		if (i!=size-1)
    			writePermutation.append(",");
    		else
    			writePermutation.append("}");
    	}
    	String permString = writePermutation.toString();
    	return permString;
    }
    /**
     * Returns a string representation of this <code>Individual</code>.
     * 
     * @return a string representation of this <code>Individual</code>
     */
    public String toString() {
    	StringBuilder writePermutation = new StringBuilder();
    	writePermutation.append("Permutation: ");
    	writePermutation.append(this.permutationToString());
    	writePermutation.append(", Fitness");
    	writePermutation.append(this.fitness);
        writePermutation.append(", size ");
        writePermutation.append(this.size);
    return writePermutation.toString();
    }
    
    /**
     * Runs a series of tests.
     * 
     * @param args command line parameters of the program
     */
    public static void main(String[] args) {

	// REPLACE THE BODY OF THIS METHOD WITH YOUR OWN IMPLEMENTATION
    for (int i=0; i<1000; i++)
    {
    Individual Sam= new Individual((int) Math.floor(i/5));
	String test=Sam.toString();
System.out.println(test);
    }
    }
}