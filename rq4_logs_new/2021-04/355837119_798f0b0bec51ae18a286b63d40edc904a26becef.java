import java.lang.Math;
import org.junit.Assert;
import org.junit.Test;


public class AreaTests {
  private Area Area = new Area();
  // Triangle
  @Test
  public void checkTriangleAreaValidInput(){
    double b = 10;
    double h = 20;
    double actual = Area.triangle(b,h);
    double expected = 100;
    Assert.assertEquals(0, Double.compare(expected, actual));
  }
  @Test
  public void checkTriangleAreaInvalidBaseInput(){
    double b = -10;
    double h = 20;
    double actual = Area.triangle(b,h);
    double expected = 0;
    Assert.assertEquals(0, Double.compare(expected, actual));
  }
  @Test
  public void checkTriangleAreaInvalidHeightInput(){
    double b = 10;
    double h = -20;
    double actual = Area.triangle(b,h);
    double expected = 0;
    Assert.assertEquals(0, Double.compare(expected, actual));
  }
  // Rectangle
  @Test
  public void checkRectangleAreaValidInput(){
    double b = 10;
    double h = 20;
    double actual = Area.rectangle(b,h);
    double expected = 200;
    Assert.assertEquals(0, Double.compare(expected, actual));
  }
  @Test
  public void checkRectangleAreaInvalidBaseInput(){
    double b = -10;
    double h = 20;
    double actual = Area.rectangle(b,h);
    double expected = 0;
    Assert.assertEquals(0, Double.compare(expected, actual));
  }
  @Test
  public void checkRectangleAreaInvalidHeightInput(){
    double b = 10;
    double h = -20;
    double actual = Area.rectangle(b,h);
    double expected = 0;
    Assert.assertEquals(0, Double.compare(expected, actual));
  }
  // Square
  @Test
  public void checkSquareAreaValidInput(){
    double s = 10;
    double actual = Area.square(s);
    double expected = 100;
    Assert.assertEquals(0, Double.compare(expected, actual));
  }
  @Test
  public void checkSquareAreaInvalidSideInput(){
    double s = -10;
    double actual = Area.square(s);
    double expected = 0;
    Assert.assertEquals(0, Double.compare(expected, actual));
  }
  // Circle
  @Test
  public void checkCircleAreaValidInput(){
    double radius = 10;
    double expected = Math.PI * Math.pow(radius, 2);
    double actual = Area.circle(radius);
    Assert.assertEquals(0, Double.compare(expected, actual));
  }
  @Test
  public void checkCircleAreaInvalidInput(){
    double radius = -10;
    double actual = Area.circle(radius);
    double expected = 0;
    Assert.assertEquals(0, Double.compare(expected, actual));
  }

}