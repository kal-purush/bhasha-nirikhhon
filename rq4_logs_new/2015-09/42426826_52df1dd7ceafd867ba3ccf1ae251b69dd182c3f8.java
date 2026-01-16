
package planetcompare;


import java.lang.Math; 
import java.math.BigDecimal; 
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.util.Scanner;
import static planetcompare.PlanetCompare.planetInput;

/******************************************************************************
 *PlanetArray()             => creates the PlanetArray
 * sortBySize()             => instance method that performs an insertion sort and lists the planets by size
 * sortByGravity()          => instance method that performs an insertion sort and lists the planets by surface gravity
 * printArray()             => prints the array. 
 * printSize()              => prints a list of the planets with the diameter.
 * printGravity()           => prints a list of the planets with the surface gravity
 * printWeight              => determines user weight and then prints a list of the planets with the user's weight on the planet
 * planetSearch(String)     => takes in a String, parses it to get two planet names and then searches
 *                              the array to see if they are present. gives the output.
 * findIt(String, array)    => searches the array to find the name and returns the corresponding index
 *                              or -1 if not present.  
 * compareArea(p1, p2)      => compares the area of two planets. Returns a BigDecimal.
 * compareVolume(p1, p2)    => compares the volume of two planets. Returns a BigDecimal. 
 * INNER CLASS******************************************************************
 * Planet(String, int, double)
 *                          => constructs a Planet using the name, diameter and surface gravity. 
 * getDiameter()            => returns the diameter as a BigInteger
 * getRadiusCubed()         => returns the radius cubed as a BigInteger. 
 * getName()                => returns the name of the Planet capitalized. 
 * getArea()                => returns the area as a BigDecimal
 * getVolume()              => returns the volume as a BigDecimal
 * userMass(double)         => returns a double, the mass extracted from the weight
 * userWeight(double)       => returns a double, the weight on the planet. 
 * getWeight(double)           => takes the earth weight, calls userMass() and userWeight() and returns 
 *                              the weight on the given planet. 
 * toString()               => returns a String for printing.
 * compareSizeTo(Planet)    => a comparator that uses diameter and returns 1, -1 or 0. 
 * compareGravityTo(Planet) => a comparator that uses gravity and returns 1, -1, or 0.
 * 
 */
public class PlanetArray {
    private Planet solarSystem[]; 
    int length = 14;
    /**
    *Constructor that creates an array of Planet with all eight planets plus six dwarf planets. 
    **/
    public PlanetArray()
    {
        solarSystem = new Planet[14]; 
        
        
        solarSystem[0] = new Planet("mercury", 4879, 3.7); 
        solarSystem[1] = new Planet("venus", 12100, 8.87); 
        solarSystem[2] = new Planet("earth", 12750, 9.80665); 
        solarSystem[3] = new Planet("mars", 6800, 3.71); 
        solarSystem[4] = new Planet("ceres", 950, 0.27); 
        solarSystem[5] = new Planet("jupiter", 142800, 24.79); 
        solarSystem[6] = new Planet("saturn", 120660, 10.4); 
        solarSystem[7] = new Planet("uranus", 51800, 8.87); 
        solarSystem[8] = new Planet("neptune", 49500, 11.15); 
        solarSystem[9] = new Planet("pluto", 3300, 0.658); 
        solarSystem[10] = new Planet("eris", 2326, 0.793); 
        solarSystem[11] = new Planet("haumea", 1960, 0.63); 
        solarSystem[12] = new Planet("makemake", 1434, 0.5); 
        solarSystem[13] = new Planet("sedna", 1000, .27); 
    }
    
    /**
    *Iterates over the array printing the name of each planet in one column and the 
    *diameter in another
    **/
    void printSize()
    {
        for (int i = 0; i < length; i++)
        {
            System.out.printf("%15s, %20d\n", solarSystem[i].getName(), solarSystem[i].getDiameter()); 
        }
    }
    
    /**
     *Iterates over the array printing each Planet name in the left column and the 
     * corresponding surface gravity in the right column
     */
    void printGravity()
    {
        for (int i = 0; i < length; i++)
        {
            System.out.printf("%15s:  %5.2f meters per second squared\n", solarSystem[i].getName(), solarSystem[i].getGravity()); 
        }
    }
    
    /**
     * Asks the user's weight, converts it to metric and then prints the planet name
     * in the left column and what the user's weight would be on each planet in the 
     * right column. 
     */
    void printWeight()
    {
        System.out.println("How much do you weigh in pounds?");
        Scanner in = new Scanner(System.in); 
        int weight = in.nextInt(); 
        double kWeight = 0.45359237 * weight; 
        System.out.printf("Your weight in kilos is %.2f.\n", kWeight);
        System.out.println("Here is your wei"
                + "ght in kilos on all the planets\n\n");
        sortByGravity(); 
        for (int i = 0; i < length; i++)
        {
            System.out.printf("%15s:  %6.2f \n", solarSystem[i].getName(), solarSystem[i].getWeight(kWeight)); 
        }
    }
    
    /**
     * Uses an insertion sort (very efficient for small amounts of data) to do an ascending
     * sort of the Planets by diameter. Calls the compareSizeTo() comparator of Planet. 
     */
    void sortBySize()
    {
         for( int p = 1; p < solarSystem.length; p++ )
        {
            Planet tmp = solarSystem[ p ];
            int j = p;

            for( ; j > 0 && tmp.compareSizeTo( solarSystem[ j - 1 ] ) < 0; j-- )
                solarSystem[ j ] = solarSystem[ j - 1 ];
            solarSystem[ j ] = tmp;
        }
    }
    /**
     * Uses an insertion sort (very efficient for small amounts of data) to do an ascending
     * sort of the Planets by surface gravity. Calls the compareGravityTo() comparator of Planet. 
     */
    void sortByGravity()
    {
        for( int p = 1; p < solarSystem.length; p++ )
        {
            Planet tmp = solarSystem[ p ];
            int j = p;

            for( ; j > 0 && tmp.compareGravityTo( solarSystem[ j - 1 ] ) < 0; j-- )
                solarSystem[ j ] = solarSystem[ j - 1 ];
            solarSystem[ j ] = tmp;
        }
    }
    
    /**
     * Method which prints out all of the planets using the toString() of Planet. Information
     * includes the diameter, surface area and volume. 
     */
    void printArray()
    {
        for (int i = 0; i < solarSystem.length; i++)
        {
            System.out.println(solarSystem[i]); 
        }
    }
    
//    /**
//     * Method used when comparing the sizes of two Planets. Takes in a String from 
//     * planetInput() and parses the String turning it into two Planet names. It then
//     * searches the array using findIt() to get the index of the array where the planet is located
//     * or -1 if not present and then gives the user the breakdown by calling compareVolume()
//     * with the two planets. 
//     * @param planets 
//     */
//    public void planetSearch(String planets)
//    {
//        Scanner str = new Scanner(planets); 
//        String firstName = capitalize(str.next()); 
//        String secondName = capitalize(str.next()); 
//        System.out.println("You typed " + firstName + " " + secondName + "\n");
//        
//        
//        int firstIndex = findIt(firstName, solarSystem); 
//        System.out.println(firstIndex);
//        int secondIndex = findIt(secondName, solarSystem); 
//        System.out.println(secondIndex);
//        if (firstIndex == -1 || secondIndex == -1)
//        {
//            planets = planetInput();
//            planetSearch(planets); 
//        }
//        System.out.println(solarSystem[firstIndex]);
//        System.out.println(solarSystem[secondIndex]);
//        System.out.println(solarSystem[firstIndex].getName() 
//                + " would fit inside " + solarSystem[secondIndex].getName() 
//                + " "+ compareVolume(solarSystem[secondIndex], 
//                        solarSystem[firstIndex]) + " times.");
//        
//    }
    
    /**
     * method that takes in two planets and then compares the areas (both BigDecimals) using 
     * the divide() of BigDecimal. 
     * @param Planet p1
     * @param Planet p2
     * @return 
     */
    public static BigDecimal compareArea(Planet p1, Planet p2)
    {
        return (p1.getArea().divide(p2.getArea(), 2, RoundingMode.CEILING)); 
    }
    
    /**
     * method that takes in two planets and then compares the volumes (both BigDecimals) using 
     * the divide() of BigDecimal. 
     * @param p1
     * @param p2
     * @return 
     */
    public static BigDecimal compareVolume(Planet p1, Planet p2)
    {
        return (p1.getVolume().divide(p2.getVolume(),2,  RoundingMode.CEILING));
        //return p2.getRadiusCubed().divide(p1.getRadiusCubed());
    }
    /**
     * takes in a possible Planet name and searches the PlanetArray to see if that Planet is 
     * present. Returns -1 if not found and the index of the Planet if found. 
     * @param newPlanet
     * @param solarSystem
     * @return 
     */
    public int findIt(String newPlanet)
    {
        for (int i = 0; i < length; i++)
        {
            if (newPlanet.equals(solarSystem[i].getName().toLowerCase()))
            {
                return i; 
            }
            
        }
        return -1; 
    }
    
    /**
     * method to print the Planet at a given index from outside
     * @param index 
     */
    public void getIndex(int index)
    {
        System.out.println(solarSystem[index].getName());
    }
    /**
     * method to compare two planets by volume from outside the array
     * @param indexOne
     * @param indexTwo 
     */
    public void compareAnyTwo(int indexOne, int indexTwo)
    {
        System.out.println(solarSystem[indexOne].getName() 
                + " would fit inside " + solarSystem[indexTwo].getName() 
                + " "+ compareVolume(solarSystem[indexTwo], 
                        solarSystem[indexOne]) + " times.");
    }
    /**
     * capitalizes the first letter of a String. 
     * @param line
     * @return 
     */
    private static String capitalize(String line)
    {
        return Character.toUpperCase(line.charAt(0)) + line.substring(1);
    }
    /**
     * Inner class of Planet. Due to the large size of both Jupiter and Saturn, 
     * BigDecimal and BigIntegers were required to correctly assign fields such as diameter
     * and calculate fields such as radius, area, and volume. 
     */
    public class Planet 
{
    private final String name; 
    private final BigInteger diameter; 
    private final BigInteger radius; 
    private final BigDecimal area; 
    private final BigDecimal volume;
    private final double surfaceGravity; 
    
    /**
     * The assignments for name, diameter and surfaceGravity are straightforward. 
     * In order to calculate the volume and area correctly, BigDecimal was needed. Radius was 
     * made into a BigDecimal as was pi. The multiply() and pow() methods of BigDecimal were called to 
     * find pi * (r ^ 2) and (4.0/3) * pi * (r ^ 3).  
     * @param newName
     * @param newDiameter
     * @param surfaceGs 
     */
    public Planet(String newName, int newDiameter, double surfaceGs)      
    {
        name = newName.toLowerCase(); diameter = new BigInteger(newDiameter + ""); 
        radius = new BigInteger((newDiameter/2) + ""); BigDecimal pi = new BigDecimal(Math.PI);
        BigDecimal bdRadius = new BigDecimal(radius); BigDecimal fourThirds = new BigDecimal( 4 / 3); 
        area = pi.multiply(bdRadius.pow(2)); 
        volume = (pi.multiply(bdRadius.pow(3))).multiply(fourThirds); surfaceGravity = surfaceGs; 
        
    }

    public BigInteger getDiameter()
    {
        return diameter; 
    }
    
    /**
     * one way to compare the volumes of two spheres is to divide the radius cubed of one by the other. 
     * @return 
     */
    public BigInteger getRadiusCubed()
    {
        return (radius.pow(3)); 
    }
    public String getName()
    {
        return capitalize(name);
       
    }
    public BigDecimal getArea()
    {
         return area; 
        //return roundIt(area); 
    }
    public BigDecimal getVolume()
    {
        return volume; 
    }
    
    public double getGravity()
    {
        return surfaceGravity; 
    }
    
    /**
     * Takes the user's earth weight and finds the mass. 
     * @param weight
     * @return 
     */
    double userMass(double weight)
    {
        return weight / 9.8; 
    }
    
    /**
     * finds the user's weight on the corresponding Planet. 
     * @param mass
     * @return 
     */
    double userWeight(double mass)
    {
        return mass * surfaceGravity; 
    }
    
    /**
     * comparator for sorting Planets by size. Uses the diameter and needs compareTo()
     * since the diameter is a BigInteger. 
     * @param other
     * @return 
     */
    int compareSizeTo(Planet other)
    {
        if (diameter.compareTo(other.diameter) > 0)
            {
                 return 1; 
            }
        else if (diameter.compareTo(other.diameter) < 0)
        {
            return -1; 
        }
        
        else
        {
            return 0; 
        }
    }
    
    /**
     * comparator for sorting the Planets by gravity. 
     * @param other
     * @return 
     */
    int compareGravityTo(Planet other)
    {
        if (surfaceGravity > other.surfaceGravity)
        {
            return 1; 
        }
        if (surfaceGravity < other.surfaceGravity)
        {
            return -1; 
        }
        else
        {
            return 0; 
        }
        
    }
    
    /**
     * outside method for calculating weight on different Planets. Calls userMass() and 
     * userWeight(). 
     * @param celcWeight
     * @return 
     */
    double getWeight(double celcWeight)
    { 
       double mass = userMass(celcWeight); 
       double newWeight =  userWeight(mass);
       return newWeight; 
    }
    
    /**
     * official toString() for all Planets. Outputs the diameter, area, and volume using commas 
     * for easy readability. 
     * @return 
     */
    public String toString()
    {
        NumberFormat form = NumberFormat.getInstance();
        return getName() + " is " + form.format(getDiameter()) + 
                " kilometers in diameter. Its " 
                + "area is " + form.format(getArea()) + 
                " kilometers squared and its volume is " +
                form.format(getVolume()) + " kilometers cubed.\n\n"; 
    }
    
//    private static BigDecimal roundIt(BigDecimal num)
//    {
//        BigDecimal roundedNum = num.setScale(2, BigDecimal.ROUND_HALF_EVEN); 
//        return roundedNum; 
//    }
    
     /**
     * capitalizes the first letter of a String. 
     * @param line
     * @return 
     */
    private String capitalize(String line)
{
  return Character.toUpperCase(line.charAt(0)) + line.substring(1);
}


}
}