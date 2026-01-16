package top.wujinxing.ch_2_basicjava.integer;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author wujinxing
 * @date 2021/4/24
 * @description
 */
public class IntegerTest {

    @Test
    public void test1() {
        Integer i = new Integer(100);
        Integer j = new Integer(100);
        Assert.assertFalse(i == j);
        Assert.assertFalse(i < j);
        Assert.assertFalse(i > j);
    }

    @Test
    public void test2() {
        Integer i = 100;
        Integer j = 100;
        Assert.assertSame(i, j);
        Assert.assertFalse(i < j);
        Assert.assertFalse(i > j);
    }
}