package Level3.reflection;

import Level3.reflection.testFramework.Processor;

public class Main {
    public static void main(String[] args) {
        try {
            Processor.start(TestSuit1.class);
        } catch (Processor.TestSuitConfigError testSuitConfigError) {
            testSuitConfigError.printStackTrace();
        }

        try {
            Processor.start(TestSuit2.class);
        } catch (Processor.TestSuitConfigError testSuitConfigError) {
            testSuitConfigError.printStackTrace();
        }
    }
}