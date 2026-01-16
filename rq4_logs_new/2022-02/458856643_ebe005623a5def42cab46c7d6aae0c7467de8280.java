package com.zerobank.pages;

import com.zerobank.utilities.Driver;
import io.cucumber.java.en.Then;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class BaseForPayeeAndAccountActivity {
    public BaseForPayeeAndAccountActivity() {
        PageFactory.initElements(Driver.get(), this);
    }

    @FindBy(xpath = "//*[@type='submit']")
    public WebElement findBtn;

    public void findSubMenuAndClick(String linkTxt){
        String path ="//a[.='"+linkTxt+"']" ;
        WebElement linkElement = Driver.get().findElement(By.xpath(path));
        linkElement.click();
    }



}