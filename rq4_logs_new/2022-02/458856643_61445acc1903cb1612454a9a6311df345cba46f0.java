package com.zerobank.pages;

import com.zerobank.utilities.Driver;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class AccountActivitiesPage {
    public AccountActivitiesPage() {
        PageFactory.initElements(Driver.get(), this);
    }

    @FindBy(xpath ="//a[.='Find Transactions']" )
    private WebElement findTransactionLink;

    public void findTransactionLinkClick(String linkTxt){
        String path ="//a[.='"+linkTxt+"']" ;
        WebElement linkElement = Driver.get().findElement(By.xpath(path));
        linkElement.click();
    }
}