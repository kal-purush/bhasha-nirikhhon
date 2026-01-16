package price.weston.amazeing;

import org.junit.Assert;
import uk.co.jemos.podam.api.PodamFactoryImpl;

/**
 * Created by wprice on 4/21/17.
 */
public class MazeTestHelper {

    public static <T> void typeComplies(Class<T> c) {

        try {
            typeComplies(c.newInstance());

        }catch(Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public static <T> void typeComplies(T t) {
        Assert.assertNotNull(t);
        Assert.assertEquals(t, t);
        Assert.assertNotNull(t.toString());
        Assert.assertNotEquals(t, null);
        Assert.assertNotEquals(t, createAndPopulate(t.getClass()));
        Assert.assertNotEquals(t.hashCode(), createAndPopulate(t.getClass()).hashCode());
    }

    private static <T> T createAndPopulate(Class<T> c) {
        return random(c);
    }

    public static <T> T random(Class<T> t) {
        PodamFactoryImpl factory = new PodamFactoryImpl();
        return factory.manufacturePojoWithFullData(t);
    }
}