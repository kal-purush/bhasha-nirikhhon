package testng_basics;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.example.CustomDataProvider;

public class TestDataProviders {
    @Test(dataProvider = "loginDetails")
    public  void login(String user, String pass){
        System.out.println("User: "+user+" "+"Password: "+pass);
    }

    @DataProvider
    public Object[][] loginDetails(){
        Object[][] data = {{"Dev","123"},{"Endra","345"},{"GOD","789"}};
        return data;
    }

    @Test(dataProvider = "loginDetails", dataProviderClass = CustomDataProvider.class)
    public void loginFromClass(String user, String pass){
        System.out.println("User: "+user+" "+"Password: "+pass);
    }
}