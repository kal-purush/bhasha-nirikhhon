package einstein;

import net.sf.javabdd.BDD;

/**
 * Created by ginva_000 on 11.12.2015.
 */
public class EinsteinTask6_2 extends EinsteinBase{
    @Override
    public void rightLimit(int property1, int value1, int property2, int value2) {
        task.andWith(locationLimit(property1, value1, property2, value2, 1, 1));
    }

    public void leftLimit(int property1, int value1, int property2, int value2) {
        task.andWith(locationLimit(property1, value1, property2, value2, -1, -1));
    }

    @Override
    public void besideLimit(int property1, int value1, int property2, int value2) {
        BDD tmp1 = locationLimit(property1, value1, property2, value2, 1, 1);
        BDD tmp2 = locationLimit(property1, value1, property2, value2, -1, -1);
        task.andWith(tmp1.orWith(tmp2));
    }
}