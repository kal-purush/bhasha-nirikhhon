package suits;

import TextExel.InvalidLogOnWithDataFromExcel;

import logOn.LoginTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


@RunWith(Suite.class)
@Suite.SuiteClasses(
        {
                LoginTest.class,
                InvalidLogOnWithDataFromExcel.class
                
        }
)
public class SuitLoginPage {

}