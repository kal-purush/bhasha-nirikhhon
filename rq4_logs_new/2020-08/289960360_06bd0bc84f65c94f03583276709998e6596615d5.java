package com.google.apps;

import org.testng.annotations.Test;
import java.io.IOException;
import static com.google.core.setup.FrameworkConfiguration.*;

public class TestClass {

    @Test
    public void testExample() throws InterruptedException, IOException {
        initialize();
        getDriver().get("https://google.com");
        Thread.sleep(5000);
        getDriver().quit();
    }

}