/*******************************************
 *
 * Author: Ryan McGuire
 * Date: 10/12/2018
 * Step 9
 *
 *
 *******************************************/

package productionline;

public class Screen implements ScreenSpec{

  String resoltion;
  int refreshRate;
  int responseTime;

  @Override
  public String getResolution() {

    return resoltion;
  }

  @Override
  public int getRefreshRate() {

    return refreshRate;
  }

  @Override
  public int getResponseTime() {

    return responseTime;
  }

  @Override
  public String toString() {
    return super.toString();
  }
}