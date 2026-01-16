package xceder;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: Mac
 * Date: 2018-02-05
 * Time: 上午11:39
 */
public class PersonTestTwo {
    private Person person;
    @Before
    public void setUp(){ {
        person = new Person("小王",22,"家里蹲大学");
    }
    }

    @Test
    public void setName() {
        person.setName("xceder");
        assertEquals(person.getName(),"xceder");
        System.out.println(person.getName());
        }


    @Test
    public void setAge() {
        person.setAge(12);
        assertNotEquals(person.getAge(),13);
    }

    @Test
    public void setSchool() {
        assertNotNull(person.getSchool());
        System.out.println("Test");
    }
}