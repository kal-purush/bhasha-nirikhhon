package class_03.entity;

import class_03.worker.Engineer;
import class_03.worker.TestEngineer;

public class AutomatedTest extends Test {

    public AutomatedTest(TestLevel testLevel, int instability) {
        super(testLevel);
        this.instability = instability;
    }

    @Override
    public Result apply(Engineer engineer) {
        int anxiety;
        if (engineer instanceof TestEngineer) {
            anxiety = engineer.getAnxiety();
        } else
            anxiety = 1;
        int result = anxiety * instability * complexity;
        if (result > 30)
            return Result.FAILED;
        else
            return Result.PASSED;
    }
}