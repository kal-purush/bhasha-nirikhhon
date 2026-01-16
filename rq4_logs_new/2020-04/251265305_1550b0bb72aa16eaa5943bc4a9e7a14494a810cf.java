package tests_engineers;

import entity.AutomatedATest;
import entity.TestLevel;
import org.junit.Assert;
import org.junit.Test;
import worker.TestEngineer;

public class BaseTestTests {
    public static final String MSG = "Test: %s; Level: %s; Instability: %d; Anxiety: %d; Skill: %d";
    TestEngineer engineer = new TestEngineer();

    @Test
    public void testEngineerComplexityUnit(){
        AutomatedATest testGui = new AutomatedATest(TestLevel.UNIT,4);
        Assert.assertEquals("Value of Complexity is not correct: ",
                1, testGui.getComplexity());
    }

    @Test
    public void testEngineerComplexityApi(){
        AutomatedATest testGui = new AutomatedATest(TestLevel.API,6);
        Assert.assertEquals("Value of Complexity is not correct: ",
                3, testGui.getComplexity());
    }

    @Test
    public void testEngineerComplexityGui(){
        AutomatedATest testGui = new AutomatedATest(TestLevel.GUI,4);
        Assert.assertEquals("Value of Complexity is not correct: ",
                9, testGui.getComplexity());
    }

}