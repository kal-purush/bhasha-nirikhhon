package com.amazon.data;

import org.testng.annotations.DataProvider;

import java.util.ResourceBundle;

public class DataProviders {
    ResourceBundle rb = ResourceBundle.getBundle("data");
    @DataProvider(name="logindata")
    public Object[][] getLoginData(){
        Object[][] data={{rb.getString("useremail"),rb.getString("password")}};
        return data;
    }
    @DataProvider(name="loginInvalidpasword")
    public Object[][] getInvalid(){
        Object[][] data={{rb.getString("useremail"),rb.getString("password1")}};
        return data;
    }
    @DataProvider(name="loginInvaliddata")
    public Object[][] getInvalidData(){
        Object[][] data={{rb.getString("useremail1"),rb.getString("password1")},
                         {rb.getString("useremail1"),rb.getString("password")}};
        return data;
    }
    @DataProvider(name="accountData")
    public Object[][] getAccountData(){
        Object[][] data={{rb.getString("name"),rb.getString("useremail1"),rb.getString("password")}};
                ;
        return data;
    }
    @DataProvider(name="searchData")
    public Object[][] searchData(){
        Object[][] data={{rb.getString("item1")},
                        {rb.getString("item2")},
                        {rb.getString("item3")},
                        {rb.getString("item4")}};

        return data;
    }
    @DataProvider(name="searchCustData")
    public Object[][] searchCusData(){
        Object[][] data={{rb.getString("useremail"),rb.getString("password"),rb.getString("item1")}};

        return data;
    }

    @DataProvider(name="searchOne")
    public Object[][] searchOne(){
        Object[][] data={{rb.getString("item2")}};

        return data;
    }
    @DataProvider(name="dpt")
    public Object[][] searchDepartment(){
        Object[][] data={{rb.getString("department1")},{rb.getString("department2")},{rb.getString("department3")}};

        return data;
    }

}