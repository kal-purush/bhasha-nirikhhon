/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package neeh;

import java.util.Arrays;

public class NEEH 
{

   
/*
This program outputs the number of effective exercises per hour (NEEH) in a day
of 15 athletes. The number of hours is between integers 3 and 11 inclusive.
It generates also statistical information in two different formats, 
column format and histogram, based on the user’s choice.
The program can also display both formats together. 
Near each NEEH value, there is a number of athletes from the total who have
the same NEEH value in the output. There is a descending display 
of the greatest number of athletes to the least number of athletes from the total 
who have the same NEEH value. Also, the program has the NEEH recorded by 
the maximum number of athletes. 
*/

    
// instance variables
private int[] NEEHvalues;
private int [] eachNEEH;
private final int aboveNEEH;
  

// methods:
// default constructor  
  public NEEH(int[]NEEHvalues) 
 {
  this.NEEHvalues = NEEHvalues;
  this.eachNEEH = new int[9];
  this.aboveNEEH = 0;// this is for a default value
 }

     
// parameterized constructor to create instances of objects with user defined
// states
  public NEEH(int[] NEEHvalues, int [] eachNEEH, int aboveNEEH) 
 {
  this.NEEHvalues = NEEHvalues;
  this.eachNEEH = eachNEEH;
  this.aboveNEEH = aboveNEEH;
 }

/* accessors (getters) this is to get accessed of the private variables for
other classes and return its values */
  public int[] getNEEHValues() 
 {
  return NEEHvalues;
 }

  public int [] getEachNEEH() 
 {
  return eachNEEH;
 }

  public int getAboveNEEH() 
 {
  return aboveNEEH;
 }

   
// mutators to control changes to the private variables:
// this mutator is for the number of effective hours possible in array
  public void setNEEHValues(int[] NEEHvalues) 
 {
  this.NEEHvalues = NEEHvalues; 
 }

    
// this mutator is for displaying the number of athletes with the same NEEH
  public int[] setEachNEEH(int[]eachNEEH)
 {
  //like mentionned above NEEH values are between 3 and 11
  eachNEEH = new int[9]; 

  /*if i is inferior to the length of the array of NEEH i is initially 0
  but increments by 1 after going through each iteration of the array*/ 
    for (int i = 0; i < NEEHvalues.length; i++) 
   {
    int value = NEEHvalues[i];//the current index i stored in the variable value
     if (value >= 3 && value <= 11)
    {
     eachNEEH[value - 3]++;
    }
        }

        return eachNEEH;
    }

    
// mutator to display for the number of athletes above the NEEH
  public int setAboveNEEH(int aboveNEEH)
 {
  // to count the values in NEEHvalues
  int count = 0;
  // to got through each element value in the array NEEHvalues
   for (int value : NEEHvalues)
  {
    if (value > aboveNEEH) 
   {
    count++;//increment by 1
   }
  }
 return count;
}
    
    
    
// toString to display in one string all variables 
  @Override
  public String toString() 
 {
  return "NEEH:" + Arrays.toString(NEEHvalues) +
                "\nEachNEEH: " + Arrays.toString(eachNEEH) +
                "\nabove" + aboveNEEH;
 }

// equals for comparing the different format objects
  public boolean equals(NEEH otherObj)
 {
  return Arrays.equals(NEEHvalues, otherObj.NEEHvalues) &&
                Arrays.equals(eachNEEH, otherObj.eachNEEH);
 }
   

    
// a method to display all the instance variables in column format
  public void DisplayColumnformat() 
 {
  eachNEEH = setEachNEEH(new int[9]);
   for (int i = 0; i < eachNEEH.length; i++) 
  {
   System.out.print("NEEH " + (i + 3) + " is " + eachNEEH[i]);
        
//if i is not the last element of the array and i is a multiple of 3 to create column
    if ((i + 1) % 3 == 0 && i < eachNEEH.length - 1)        
   {
    System.out.println();
   }
    else 
   {
    System.out.print("           ");//to add space
   }
  }
 }
        
    

// a method to display all the instance variables in histogram format
  public void DisplayHistogramformat()
 {
  eachNEEH = setEachNEEH(new int[9]);
  System.out.println("NEEH Value: Number of Athetes");
   for (int i = 0; i < eachNEEH.length; i++) 
  {
    System.out.print("         " + (i + 3) + ": ");
    for (int j = 0; j < eachNEEH[i]; j++) 
   {
    System.out.print("*");
   }
    System.out.println();
  }
 }
       

//a mehod to display the NEEH recorded by the maximum number of athletes
   public int NEEHbyMaxAthletes() 
  {     
   eachNEEH = setEachNEEH(new int[9]);
   int maxNum = 0;

    for (int i = 1; i < eachNEEH.length; i++) 
   {
     if (eachNEEH[i] > eachNEEH[maxNum]) 
    {
      maxNum = i;
    }
   }
// Adding 3 to get the actual NEEH value
   return maxNum + 3; 
 }
}
