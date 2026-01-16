package is.ru.stringcalculator;
//import static org.hamcrest.MatcherAssert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

public class TictactoeTest {

    public static void main(String args[]) {
	org.junit.runner.JUnitCore.main("is.ru.stringcalculator.CalculatorTest");
    }
                    
    @Test
    public void testEmptyString(){
	assertEquals(0, Tictactoe.add(""));
    }
    
}