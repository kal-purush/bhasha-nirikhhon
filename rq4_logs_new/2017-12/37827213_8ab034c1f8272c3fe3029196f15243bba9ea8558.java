package com.FactoryVSObject.factory;

import org.openqa.selenium.Keys;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

public class GooglePageFactoryHelper {

    @FindBy(id = "lst-ib")
    private WebElement searchField;

    @FindBy(how = How.LINK_TEXT, using = "Halle Berry - Wikipedia")
    private WebElement wikiHalleBeryLink;

    public void findPage(String query){
        searchField.sendKeys(query, Keys.ENTER);
//        searchField.submit();
        wikiHalleBeryLink.click();
    }
}