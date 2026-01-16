package einstein;

import net.sf.javabdd.BDD;

/**
 * Created by ginva_000 on 11.12.2015.
 */
public class EinsteinTask9_1 extends EinsteinBase{
    public void upperLimit(int property1, int value1, int property2, int value2) {
        task.andWith(locationLimit(property1, value1, property2, value2, -1, 1));
    }

    public void lowerLimit(int property1, int value1, int property2, int value2) {
        task.andWith(locationLimit(property1, value1, property2, value2, 1, 1));
    }

    @Override
    public void besideLimit(int property1, int value1, int property2, int value2) {
        BDD tmp1 = locationLimit(property1, value1, property2, value2, -1, 1);
        System.out.println("tmp1");
        BDD tmp2 = locationLimit(property1, value1, property2, value2, 1, 1);
        System.out.println("tmp2");
        tmp1.orWith(tmp2);
        System.out.println("or");
        task.andWith(tmp1);

    }

    @Override
    public void limit7() {
        //type 1
        BDD[] tmp = new BDD[K_SIZE];
        BDD[] tmpUpper = new BDD[K_SIZE];
        BDD[] tmpLower = new BDD[K_SIZE];
        BDD[] sum = new BDD[K_SIZE];
        toBinary(sum, 0);
        toBinary(tmp, 0);
        toBinary(tmpUpper, 0);
        toBinary(tmpLower, 0);
        int item;

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < columns; j++) {
                for (int property = 0; property < M; property++) {
                    for (int bin = 0; bin < LOG_N; bin++) {
                        item = i*columns + j;
                        tmp[bin] = factory.ithVar((M * item + property) * LOG_N+bin);
                        if (i > 0 && j < columns-1) {
                            item = (i-1)*columns + j+1;
                            tmpUpper[bin] = factory.ithVar((M * item + property) * LOG_N+bin);
                        }
                        if (i < rows-1 && j < columns-1) {
                            item = (i+1)*columns + j+1;
                            tmpLower[bin] = factory.ithVar((M * item + property) * LOG_N+bin);
                        }
                    }
                    sum = sum(sum, tmp);
                    sum = sum(sum, tmpLower);
                    sum = sum(sum, tmpUpper);
                }
                task.andWith(compare(k,sum));
                toBinary(sum,0);
                toBinary(tmpLower, 0);
                toBinary(tmpUpper, 0);
            }
        }
    }
}