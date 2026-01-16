package challenges;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.JavascriptExecutor;
import static java.lang.System.out;

public class Test_N extends ParentForTests {

    private static String source;
    private final static String stringToFind = "news";
    protected int count;

    @BeforeClass
    public static void setDriver() {
        setChromeDriver();
    }

    @AfterClass
    public static void closeDriver() {
        driverChrome.quit();
    }

    @Test
    public void test_N1() throws InterruptedException {
        //get source after document dom is loaded
        driverChrome.get("https://themarker.com");
        JavascriptExecutor js = (JavascriptExecutor) driverChrome;
        source = documentReadyComplete(driverChrome,js);
        // count "news" accurances on page
        count = findOccurance(source, stringToFind);
        out.println("the length of page source string  = "+source.length());
        out.println("World news exists on page - "+count+" times");
    }
}