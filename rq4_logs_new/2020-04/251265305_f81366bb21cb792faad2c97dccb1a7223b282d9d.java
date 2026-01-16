package class_03.test;

import class_03.entity.AutomatedTest;
import class_03.entity.ManualTest;
import class_03.entity.TestLevel;
import class_03.worker.AutomationEngineer;
import org.junit.Assert;
import org.junit.Test;

import static class_03.entity.Result.FAILED;
import static class_03.entity.Result.PASSED;

/*
We will do the different tests depending on values of complexity and instability
to cover all the possible situations.
 */
public class AutomationEngineerTests {

    @Test
    public void autoTest_Unit_Api_Gui(){
        AutomationEngineer engineer = new AutomationEngineer();
        AutomatedTest testUnit = new AutomatedTest(TestLevel.UNIT,10);
        AutomatedTest testApi = new AutomatedTest(TestLevel.API,10);
        AutomatedTest testGui1 = new AutomatedTest(TestLevel.GUI,3);
        AutomatedTest testGui2 = new AutomatedTest(TestLevel.GUI,4);
        Assert.assertEquals("UNIT automated test, instability=10", PASSED, engineer.executeTest(testUnit));
        Assert.assertEquals("API automated test, instability=10", PASSED, engineer.executeTest(testApi));
        Assert.assertEquals("GUI automated test, instability=3", PASSED, engineer.executeTest(testGui1));
        Assert.assertEquals("GUI automated test, instability=4", FAILED, engineer.executeTest(testGui2));
    }


    @Test
    public void manualTest_Unit_Api(){
        AutomationEngineer engineer = new AutomationEngineer();
        ManualTest testUnit = new ManualTest(TestLevel.UNIT,10);
        ManualTest testApi1 = new ManualTest(TestLevel.API,3);
        ManualTest testApi2 = new ManualTest(TestLevel.API,4);
        Assert.assertEquals("UNIT manual test, instability=10", PASSED, engineer.executeTest(testUnit));
        Assert.assertEquals("API manual test, instability=3", PASSED, engineer.executeTest(testApi1));
        Assert.assertEquals("API manual test, instability=4", FAILED, engineer.executeTest(testApi2));
    }

    @Test
    public void manualTest_Gui(){
        AutomationEngineer engineer = new AutomationEngineer();
        ManualTest testGui1 = new ManualTest(TestLevel.GUI,1);
        ManualTest testGui2 = new ManualTest(TestLevel.GUI,2);
        Assert.assertEquals("GUI manual test, instability=1", PASSED, engineer.executeTest(testGui1));
        Assert.assertEquals("GUI manual test, instability=2", FAILED, engineer.executeTest(testGui2));
    }
}