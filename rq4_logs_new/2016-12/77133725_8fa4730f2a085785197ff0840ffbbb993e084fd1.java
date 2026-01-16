package ex12Q2;

/**
 * Represents a possible solution to the three-way matching optimization problem.
 * @author Yaniv Tzur
 */
public class Chromosome 
{
	public static int NUM_OF_GENES = 3; // The number of components/genes in each chromosome.
	private static int NUM_OF_PREFERENCE_LISTS = 2; // Number of preference lists.
	private static int NUM_OF_COMPONENTS = 3; // Number of components in the chromosome.
	
	private Man _man; // Represents a man in a solution for the three-way matching optimization problem.
	private Woman _woman; // Represents a man in a solution for the three-way matching optimization problem.
	private Dog _dog; // Represents a man in a solution for the three-way matching optimization problem.
	
	/**
	 * Creates a new chromosome (solution) with the input man, woman and dog components (genes).
	 * @param man The man component (gene) to store in the chromosome.
	 * @param woman The woman component (gene) to store in the chromosome.
	 * @param dog The dog component (gene) to store in the chromosome.
	 */
	public Chromosome(Man man, Woman woman, Dog dog)
	{
		setMan(man, woman, dog);
		setWoman(woman, man, dog);
		setDog(dog, man, woman);
	}
	
	/**
	 * Sets the man component (gene) of the chromosome to the input man component and sets its
	 * partners to be the input woman and dog.
	 * @param man The man to set as the man component (gene) of the chromosome.
	 * @param woman The woman to set as one of the man's partners.
	 * @param dog The dog to set as one of the man's partners.
	 */
	public void setMan(Man man, Woman woman, Dog dog)
	{
		man.setPartners(woman, dog);
		setMan(man);
	}
	
	/**
	 * Sets the woman component (gene) of the chromosome to the input woman component and sets its
	 * partners to be the input man and dog.
	 * @param woman The woman to set as the woman component (gene) of the chromosome.
	 * @param man The man to set as one of the woman's partners.
	 * @param dog The dog to set as one of the woman's partners.
	 */
	public void setWoman(Woman woman, Man man, Dog dog)
	{
		woman.setPartners(man, dog);
		setWoman(woman);
	}
	
	/**
	 * Sets the dog component (gene) of the chromosome to the input dog component and sets its
	 * partners to be the input man and woman.
	 * @param dog The dog to set as the dog component (gene) of the chromosome.
	 * @param man The man to set as one of the dog's partners.
	 * @param woman The woman to set as one of the dog's partners.
	 */
	public void setDog(Dog dog, Man man, Woman woman)
	{
		dog.setPartners(man, woman);
		setDog(dog);
	}
	
	/**
	 * Returns a reference to the man component of the three-way matching (chromosome).
	 * @return A reference to the man component of the three-way matching (chromosome).
	 */
	public Man getMan()
	{
		return _man;
	}
	
	/**
	 * Sets the input man as the chromosome's man component.
	 * @param man The man to set as the man component for the chromosome.
	 */
	public void setMan(Man man)
	{
		_man = man;
	}
	
	/**
	 * Returns a reference to the woman component of the three-way matching (chromosome).
	 * @return A reference to the woman component of the three-way matching (chromosome).
	 */
	public Woman getWoman()
	{
		return _woman;
	}
	
	/**
	 * Sets the input woman as the chromosome's man component.
	 * @param woman The woman to set as the woman component for the chromosome.
	 */
	public void setWoman(Woman woman)
	{
		_woman = woman;
	}
	
	/**
	 * Returns a reference to the dog component of the three-way matching (chromosome).
	 * @return A reference to the dog component of the three-way matching (chromosome).
	 */
	public Dog getDog()
	{
		return _dog;
	}
	
	/**
	 * Sets the input dog as the chromosome's man component.
	 * @param dog The dog to set as the dog component for the chromosome.
	 */
	public void setDog(Dog dog)
	{
		_dog = dog;
	}
	
	/**
	 * Receives the number of chromosomes in the current generation and returns the fitness
	 * of the chromosome.
	 * The fitness is calculated as the maximum possible sum of distances minus the actual sum of
	 * distances to obtain a monotonically increasing function.
	 * @param numOfChromosomes The number of chromosomes in the current generation.
	 * @return The fitness of the chromosome.
	 */
	public int getFitness(int numOfChromosomes)
	{
		return (numOfChromosomes * NUM_OF_PREFERENCE_LISTS * NUM_OF_COMPONENTS
				- 
				(_man.getDistance() 
				+ 
				_woman.getDistance() 
				+ 
				_dog.getDistance()));
	}
}