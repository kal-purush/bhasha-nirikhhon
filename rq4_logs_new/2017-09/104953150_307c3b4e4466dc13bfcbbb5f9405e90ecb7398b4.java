import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Scanner;

import student.TestCase;

/**
 * @author ianimp96
 * @author nickeda
 * @version 9/19/17 (5:32 PM)
 * 
 *          The RectangleTest class will test the different methods in the
 *          Rectangle class.
 *
 */
public class Rectangle1Test extends TestCase{

    private Rectangle1 rect;
    private String[] args;
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    
    public void setUp() throws Exception {
        rect = new Rectangle1();
        args = new String[1];
        System.setErr(new PrintStream(errContent));
    }

    
    public void test() {
        
        args[0] = ("Test.txt");
        
        // test Exceptions
        Exception caught = null;
        try {
           Rectangle1.main(args);;
        }
        catch (Exception e){
            caught = e;
        }
        assertNotNull(caught);
        assertFalse(caught instanceof FileNotFoundException);
        
        
        // test output
        Scanner scan = new Scanner("TestOutput.txt");
        scan.useDelimiter("\n");
        
        while (scan.hasNext()) {
            
        }

        scan.close();
        
    }
    
    public void testErrors() {
        args = new String[0];
        Rectangle1.main(args);
        assertTrue(errContent.toString().contains("Check arguments"));
    }

}