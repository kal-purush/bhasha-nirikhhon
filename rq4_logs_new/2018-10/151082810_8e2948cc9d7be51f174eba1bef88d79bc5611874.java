/*******************************************
 *
 * Author: Ryan McGuire
 * Date: 09/29/2018
 * Step 3
 *
 *
 *******************************************/

package productionline;

import java.util.Date;

public abstract class Product implements Item {

  //Create an abstract type called Product that will implement the Item interface.
  // Product will implement
  //the basic functionality that all items on a production line should have

  private int serialNumber;
  private Date manufacuredOn;
  private String name;
  private String manufacturer;

  private static int currentProductionNumber = 1;


  //Complete the methods from the interface Item.

  //the program requires we use a class variable.
  // So using a static field in an instance method is unavoidable right now.
  @Override
  public void setProductionNumber(int productionNumber) {

    currentProductionNumber = productionNumber;
  }

  @Override
  public void setName(String name) {

    this.name = name;
  }

  @Override
  public String getName() {

    return name;
  }

  //The project specifies we do this for the function.
  @Override
  public Date getManufactureDate() {

    return manufacuredOn;
  }

  @Override
  public int getSerialNumber() {

    return serialNumber;
  }

  //Add a constructor that will take in the name of the product and set
  // this to the field variable name. You
  //will also assign a serial number from the currentProductionNumber.
  // The currentProductionNumber
  //should be incremented in readiness for the next instance.
  //Set manufacturedOn as the current date and time.

  /*** <p></p>
   * This Construct sets the name of the product and assigns a
   * serial number and manufactured on date to the product.
   * @param productName The product that is being modified.
   *
   * */
  public Product(String productName) {

    this.name = productName;
    serialNumber = currentProductionNumber;

    //the program requires we use a class variable.
    // So using a static field in an instance method
    // is unavoidable right now.
    currentProductionNumber++;
    manufacuredOn = new Date(System.currentTimeMillis());
    manufacturer = Item.manufacturer;

  }

  @Override
  public String toString() {

    return "Manufacturer : "
        + manufacturer
        + "\n"
        + "Serial Number : "
        + serialNumber
        + "\n"
        + "Date : "
        + manufacuredOn
        + "\n"
        + "Name : "
        + name
        + "\n";
  }
}