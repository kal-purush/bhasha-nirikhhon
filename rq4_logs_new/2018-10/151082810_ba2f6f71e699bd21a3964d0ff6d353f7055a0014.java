/*******************************************
 *
 * Author: Ryan McGuire
 * Date: 10/13/2018
 * Step 11
 *
 *
 *******************************************/

package productionline;

public class MoviePlayer extends Product implements MultimediaControl {

  private Screen screen;
  private MonitorType monitorType;

  /***
   *
   * This constructor takes in the name of the product from
   * the constructor in the super class (Product).*
   * @param productName name of the product.
   * @param screen takes in the screen resoltion, refresh rate, and response time.
   * @param monitorType the type of the monitor.
   *
   **/
  public MoviePlayer (String productName, Screen screen, MonitorType monitorType){

    super(productName);
    this.screen = screen;
    this.monitorType = monitorType;
  }

  @Override
  public void play() {

    System.out.println("Playing");
  }

  @Override
  public void stop() {

    System.out.println("Stopped");
  }

  @Override
  public void previous() {

    System.out.println("Previous");
  }

  @Override
  public void next() {

    System.out.println("Next");
  }

  @Override
  public String toString() {

    return super.toString()
        + "Monitor Type : " + monitorType + "\n"
        + "Screen Details : " + screen;
  }
}