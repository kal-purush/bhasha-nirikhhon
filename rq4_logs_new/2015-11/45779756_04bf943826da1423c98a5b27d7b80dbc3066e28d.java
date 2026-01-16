package codingtests.fizzbuzz;

import org.junit.Test;

public class FizzBuzzArgTest {

    @Test(expected = IllegalArgumentException.class)
    public void argumentCheckShouldBeInvalidForMoreThanOneArg(){
        new FizzBuzzBasic().checkCommandLineArguments(new String[]{"1","2"});
    }

    @Test(expected = IllegalArgumentException.class)
    public void argumentCheckShouldBeInvalidForNotANumberArg(){
        new FizzBuzzBasic().checkCommandLineArguments(new String[]{"fifteen"});
    }

    @Test
    public void argumentCheckShouldBeValidForANumberArg(){
        new FizzBuzzBasic().checkCommandLineArguments(new String[]{"14"});
        // no exception
    }

}