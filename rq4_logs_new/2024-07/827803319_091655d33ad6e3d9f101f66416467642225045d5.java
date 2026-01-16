///// DO NOT SUBMIT THIS FILE TO GRADESCOPE!
/**
 * This class models Bottle objects produced, filled, and capped in a Bottle Factory Chain.
 *
 */
public class Bottle {

  private static int bottleCounter = 0; // shared counter for bottles in factory
  private final int BOTTLE_ORDER; // order of production/creation of this Bottle. The order of the
                                  // first created bottle is 1.

  private String color; // color of this Bottle
  private boolean isFilled; // bottle fill status
  private boolean isCapped; // bottle seal status



  /**
   * Constructs an empty bottle, assigns a serial number using the bottle color and order
   * 
   * @param color string defining the bottle's color
   * @throws IllegalArgumentException when the color is invalid (null or empty or blank)
   */
  public Bottle(String color) throws IllegalArgumentException {
    if (color == null || color.isEmpty() || color.isBlank()) {
      throw new IllegalArgumentException("color cannot be empty");
    }

    this.color = color;
    isFilled = false;
    isCapped = false;
    bottleCounter++;
    BOTTLE_ORDER = bottleCounter;
  }

  /**
   * Returns the serialNumber of this bottle using its order of production and color<BR>
   * Example: SN1Blue
   * 
   * @return string formatted as follows<BR>
   *         "SN" + BOTTLE_ORDER + color
   */
  public String getSerialNumber() {
    return "SN" + BOTTLE_ORDER + color;
  }

  /**
   * Resets the bottle counter to zero. This method is used each time a new Bottle supply line is
   * created.
   */
  public static void resetBottleCounter() {
    bottleCounter = 0;
  }

  /**
   * Checks whether the bottle is filled
   * 
   * @return true if this bottle is filled.
   */
  public boolean isFilled() {
    return isFilled;
  }

  /**
   * Sets the bottle status to filled
   */
  public void fillBottle() {
    this.isFilled = true;
  }

  /**
   * Checks whether this bottle is Capped
   * 
   * @return true if this bottle is Capped
   */
  public boolean isCapped() {
    return isCapped;
  }

  /**
   * Caps the bottle, only if it is filled
   * 
   * @throws IllegalStateException if the bottle is empty, it cannot be capped
   */
  public void sealBottle() throws IllegalStateException {
    if (!(this.isFilled)) {
      throw new IllegalStateException("Empty bottle cannot be capped");
    }
    this.isCapped = true;
  }


  /**
   * Checks whether this Bottle equals another specific object
   * 
   * @param o other object to compare to
   * @return true if o is a Bottle and has the exact same serial number as this Bottle.
   */
  @Override
  public boolean equals(Object o) {
    return o instanceof Bottle && this.getSerialNumber().equals(((Bottle) o).getSerialNumber());
  }

  /**
   * Returns a string representation of the bottle in the format
   * "(SerialNumber):Filled/Empty:Capped/Open"
   * 
   * @return a String representation of the Bottle
   */
  @Override
  public String toString() {
    // add examples in the write-up!
    return this.getSerialNumber() + ":" + (isFilled ? "Filled" : "Empty") + ":"
        + (isCapped ? "Capped" : "Open");
  }


}