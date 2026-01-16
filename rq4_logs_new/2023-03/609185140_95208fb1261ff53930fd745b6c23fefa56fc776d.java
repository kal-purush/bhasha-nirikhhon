package testng_basics;

import org.testng.annotations.Test;

public class TestNgAttributes {
    @Test(description = "This is Testng Description Example")
    public void descriptionAttribute(){
        System.out.println("Testng Descrtiption attribute");
    }

    @Test
    public void dependsOnMain(){
        System.out.println("This is the main function");
    }

    @Test(dependsOnMethods = {"dependsOnMain"})
    public void dependentMethod(){
        System.out.println("Dependend method");
    }

    @Test(timeOut = 100)
    public void timesOutExampe() throws InterruptedException {
        System.out.println("Enters to timeout method");
        Thread.sleep(200);
        System.out.println("Exited from timeoutmethod");
    }
}