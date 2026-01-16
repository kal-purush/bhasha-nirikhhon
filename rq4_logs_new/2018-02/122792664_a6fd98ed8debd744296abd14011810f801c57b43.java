/**
 *@author Mark Khramov
 *Class: CSIS-1410
 *Assignment: A04 Interface 
 */
package a04Interface;

/**
 * This class is used to declare and initialize circle objects.
 */
public class Circle implements Shape 
{
    
	private final int radius;
    
	/**
	 * Constructor for the Circle class.
	 * 
	 * @param radius Radius of this Circle.
	 */
    public Circle(int radius) 
    {
		super();
		this.radius = radius;
	}
    
    /**
     * Returns the diameter of this Circle.
     * 
     * @return The diameter of this Circle.
     */
    public double diameter() 
    {
    	return 2 * radius;
    }
    
    /**
     * Returns the circumference of this Circle.
     * 
     * @return The circumference of this Circle.
     */
    public double circumference() 
    {
    	return 2 * Math.PI * radius;
    }
    
    /**
     * Returns the radius of this Circle.
     * 
     * @return The radius of this Circle
     */
    public int getRadius() 
    {
    	return radius;
    }
    
    /**
     * Returns a string representation of this Circle object.
     */
    @Override
    public String toString() 
    {
    	return "Circle(" + radius + ")";
    }
    
	@Override
	public double perimeter()
	{
		// TODO Auto-generated method stub
		return 2 * Math.PI * radius;
	}

	@Override
	public double area()
	{
		// TODO Auto-generated method stub
		return Math.PI * Math.pow(radius, 2);
	}
}