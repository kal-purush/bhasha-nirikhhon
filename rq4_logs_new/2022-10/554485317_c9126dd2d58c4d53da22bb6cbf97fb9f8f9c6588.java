/* Jayne Lehenbauer
L00170184 */

// import scanner class for input
import java.util.Scanner;

public class lyitElectric4
{
   public static void main(String[]args)
   {
      //declare variables
      Scanner keyboardIn = new Scanner(System.in);
      char customerType;
      int unitsEntered = 0;
      double totalUnits = 0;
      char meterType;
      int years;
      int noOfBusinesses;
      int counter;
      double standingCharge = 0;
      double excessUnits;
      double standardUnits;
      double businessUnits;
      double vat =0;
      double grandTotal = 0;
      double totalUnitCost = 0;
      int januaryDiscount = 200;
      double discountApplied = 0;
      double net;
      
      {
         System.out.println("What type of customer are you? Residential (R) or Business(B)");
         // store the next character typed as a variable
         customerType = keyboardIn.next().charAt(0);
         // if statement for customer type
         if (customerType == 'R' | customerType == 'r')
         {
            System.out.println("How many units did you use this month?");
            // store the next integer typed as a variable
            unitsEntered = keyboardIn.nextInt();
            /* I want to keep unitsEntered variable as it was entered, but I need to do calculations on the amount so making
            a new variable to store the amount and make calculations without affecting the original value */
            totalUnits = unitsEntered;
            System.out.println("What type of meter do you have? Urban (U) or Rural (R)");
            // store the next character typed as a variable
            meterType = keyboardIn.next().charAt(0);
            // nested if statement for meter type. This is for Residential customers with an Urban meter
            if (meterType == 'U' | meterType == 'u')
            {
               // standing charge amount for Urban customer
               standingCharge = 17.50;
               System.out.println("How many years have you been a customer with us?");
               // store the next integer as a variable
               years = keyboardIn.nextInt();
               // nested if statement for whether a customer was with the company for more than three years or not
               if (years >= 3)
               {
                  System.out.println("Congratulations! You are eligible for a reduction of up to 200 units this month");
                  /* nested if statement for discount to be applied. If customer entered less than 200 units then taking 200 
                  away would give a negative amount. There should only be a minimum of 0 units being charged. This calculation 
                  also takes into account the different costs for units under/over 200  */
                  if (totalUnits>200)
                  {
                     discountApplied = 200.00;
                     totalUnits = totalUnits-januaryDiscount;
                     if (totalUnits>200)
                     {
                        excessUnits = (totalUnits-200) * 0.23;
                        standardUnits = 200 * 0.18;
                        totalUnitCost = excessUnits + standardUnits;
                     }
                     else
                     {
                        totalUnitCost = totalUnits * 0.18;
                     }
                  }
                  /* else statement related to the nested if statement for discount to be applied. This is for people who 
                  less than 200 units so the January discount will reduce their units to 0*/
                  else
                  {
                     discountApplied = totalUnits;
                     totalUnitCost = 0;
                  }
               }
               /* else statement related to the nested if statement for whether a customer was with the company for more than 
               three years or not */
               else
               {
                  // nested if statement to take into account the different costs for units under/over 200
                  if (totalUnits>200)
                  {
                     excessUnits = (totalUnits - 200) *0.23;
                     standardUnits = 200 * 0.18;
                     totalUnitCost = excessUnits + standardUnits;
                  }
                  // else statement related to the above as if total units are under 200 then all units will be 18c each  
                  else
                  {
                     totalUnitCost = totalUnits * 0.18;
                  }                
               }
               // vat calculation
               vat = ((totalUnitCost + standingCharge)/100)*13.5;
               // net calculation
               net = totalUnitCost + standingCharge;
               // gross calculation
               grandTotal = totalUnitCost + standingCharge + vat;
               System.out.println("\nElectric Bill for month ending 31st January 2022");
               System.out.println("Total Units: " + unitsEntered);
               System.out.println("Total Units Discounted: -" + discountApplied);
               System.out.println("\nTotal Units to be charged: " + (unitsEntered-discountApplied));
               System.out.println("Total Unit Cost: �" + totalUnitCost);
               System.out.println("Standing Charge: �" + standingCharge);
               System.out.println("Total net Charge: �" + net);
               System.out.println("VAT at 13.5%: �" + vat); 
               System.out.println("Your total payment due is �" + grandTotal);
            }
            // else if statement related to meter type. This is for Residential customers with a Rural meter
            else if (meterType == 'R' | meterType == 'r')
            {
               // standing charge amount for Rural customer
               standingCharge = 21.50;
               System.out.println("How many years have you been a customer with us?");
               // store next int as variable
               years = keyboardIn.nextInt();
               // nested if statement for whether a customer was with the company for more than three years or not
               if (years >= 3)
               {
                  System.out.println("Congratulations! You are eligible for a reduction of up to 200 units this month");
                  /* nested if statement for discount to be applied. If customer entered less than 200 units then taking 200 
                  away would give a negative amount. There should only be a minimum of 0 units being charged. This calculation 
                  also takes into account the different costs for units under/over 200  */
                  if (totalUnits>200)
                  {
                     discountApplied = 200.00;
                     totalUnits = totalUnits-januaryDiscount;
                     if (totalUnits>200)
                     {
                        excessUnits = (totalUnits-200) * 0.23;
                        standardUnits = 200 * 0.18;
                        totalUnitCost = excessUnits + standardUnits;
                     }
                     else
                     {
                        totalUnitCost = totalUnits * 0.18;
                     }
                  }
                  /* else statement related to the nested if statement for discount to be applied. This is for people who 
                  less than 200 units so the January discount will reduce their units to 0*/
                  else
                  {
                     discountApplied = totalUnits;
                     totalUnitCost = 0;
                  }
               }
               /* else statement related to the nested if statement for whether a customer was with the company for more than 
               three years or not */
               else
               {
                  // nested if statement to take into account the different costs for units under/over 200
                  if (totalUnits>200)
                  {
                     excessUnits = (totalUnits - 200) *0.23;
                     standardUnits = 200 * 0.18;
                     totalUnitCost = excessUnits + standardUnits;
                  }
                  // else statement related to the above as if total units are under 200 then all units will be 18c each  
                  else
                  {
                     totalUnitCost = totalUnits * 0.18;
                  }                
               }
               // vat calculation
               vat = ((totalUnitCost + standingCharge)/100)*13.5;
               // net calculation
               net = totalUnitCost + standingCharge;
               // gross calculation
               grandTotal = totalUnitCost + standingCharge + vat;
               System.out.println("\nElectric Bill for month ending 31st January 2022");
               System.out.println("Total Units: " + unitsEntered);
               System.out.println("Total Units Discounted: -" + discountApplied);
               System.out.println("\nTotal Units to be charged: " + (unitsEntered-discountApplied));
               System.out.println("Total Unit Cost: �" + totalUnitCost);
               System.out.println("Standing Charge: �" + standingCharge);
               System.out.println("Total net Charge: �" + net);
               System.out.println("VAT at 13.5%: �" + vat); 
               System.out.println("Your total payment due is �" + grandTotal);
            }
         // else statement related to meter type. This is if someone enters an incorrect option
         else
            {
               System.out.println("Invalid option.");
            }
         }
         // else if statement related to customer type. This is for business customers 
         else if (customerType == 'B' | customerType == 'b')
         {
            totalUnits = 0;
            System.out.println("How many businesses do you have with us?");
            // store next integer as a variable
            noOfBusinesses = keyboardIn.nextInt();
            /* for statement to ask for the units of each business based on the number of businesses entered by the 
            customer. The units for each business will be added to the totalUnits variable during each loop and at the
            end there will be a total number of units for all businesses combined */
            for (counter = 1; counter <= noOfBusinesses; counter++)
            {
               System.out.println("How many units were used for business number" + counter + "?");
               // store next integer as variable
               businessUnits = keyboardIn.nextInt();
               totalUnits = totalUnits + businessUnits;
               totalUnitCost = totalUnits * 0.25;
            }
            // standing charge for business customers
            standingCharge = 20.00;
            // vat calculation
            vat = ((totalUnitCost + standingCharge)/100)*13.5;
            // net calculation
            net = totalUnitCost+standingCharge;
            // gross calculation
            grandTotal = totalUnitCost + standingCharge + vat;
            System.out.println("Electric Bill for month ending 31st January 2022");
            System.out.println("Total Units: " + totalUnits);
            System.out.println("Total Unit Cost: �" + totalUnitCost);
            System.out.println("Standing Charge: �" + standingCharge);
            System.out.println("Total net Charge: �" + net);
            System.out.println("VAT at 13.5%: �" + vat); 
            System.out.println("Your total payment due is �" + grandTotal + ".");
         }   
         // else statement related to customer type. This is for if someone enters an incorrect option        
         else
         {
            System.out.println("Invalid");
         }
      }
   }    
}