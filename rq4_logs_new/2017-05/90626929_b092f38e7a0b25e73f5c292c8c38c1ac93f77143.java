import org.junit.Test;
import static org.junit.Assert.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import com.diffblue.java_testcase.Reflector;

public class SearchTest {

  /* testedClasses: Search */


  /*
  * This test covers `void <init>()' block 1 (line 5)
  * This test covers `void <init>()' block 2 (line 5)
  */

  @org.junit.Test
  public void com_diffblue_javatest_Search__init__4476ae74c7082ebb_000() throws Exception {

    /* creating new object to test constructor */
    Class<?> c = Reflector.forName("com.diffblue.javatest.Search");
    Constructor<?> ctor = c.getDeclaredConstructor();
    ctor.setAccessible(true);
    Object instance=ctor.newInstance();

    /* Method return type is void, or not expected to return */
  }


  /*
  * This test covers `boolean contains(int [], int)' block 1 (line 10)
  * This test covers `boolean contains(int [], int)' block 2 (line 13)
  * This test covers `boolean contains(int [], int)' block 3 (line 14)
  * This test covers `boolean contains(int [], int)' block 4 (line 15)
  * This test covers `boolean contains(int [], int)' block 5 (line 13)
  * This test covers `boolean contains(int [], int)' block 6 (line 19)
  */

  @org.junit.Test
  public void com_diffblue_javatest_Search_contains_d74ae9083c744d1_000() throws Exception {

    /* initialize test parameters */
    com.diffblue.javatest.Search param_0 = (com.diffblue.javatest.Search) Reflector.getInstance("com.diffblue.javatest.Search");
    int [] dynamic_2_array = new int []{ 4160 };
    int [] dynamic_object1 = (int [])dynamic_2_array;
    int [] array = (int [])dynamic_object1;
    int target = 4160;

    /* call function under test */
    Class<?> c = Reflector.forName("com.diffblue.javatest.Search");
    Method m = c.getDeclaredMethod("contains", Reflector.forName("int []"), Reflector.forName("int"));
    m.setAccessible(true);
    boolean retval = (boolean) m.invoke(param_0, array, target);

    /* check return value */
    assertEquals(true, retval);
  }

}