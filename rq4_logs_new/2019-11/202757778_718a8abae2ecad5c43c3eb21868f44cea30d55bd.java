package week_8.brad.extraLongFactorials;

import org.junit.Test;

import java.math.BigInteger;

import static org.junit.Assert.*;

/**
 * Created by betterfly
 * Date : 2019.11.05
 */
public class SolutionTest {

    @Test
    public void 피보나치(){
        BigInteger result = Solution.extraLongFactorials(5);
        System.out.println(result);
    }
}