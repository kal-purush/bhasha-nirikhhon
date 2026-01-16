package example;

import org.testng.annotations.DataProvider;

public class StaticProvider {
    @DataProvider(name = "dataproviderPositive")
    public Object[][] dataTestPositiveCase() {
        return new Object[][] {
                { "Rudy", 10 },
                { "Sari", 20 },
                { "Joko", 25 }
        };
    }

    @DataProvider(name = "dataproviderNegative")
    public Object[][] dataTestNegativeCase() {
        return new Object[][] {
                { "Rudy", 10 },
                { "Sari", 20 },
                { "Joko", 25 }
        };
    }
}