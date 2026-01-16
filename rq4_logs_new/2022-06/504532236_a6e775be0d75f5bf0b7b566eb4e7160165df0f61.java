package com.one.ui.pages;

import com.one.framework.Browser;

import static com.one.locators.ClassName.INVENTORY_ITEM_NAME;
import static com.one.locators.Id.CHECKOUT;

public class Cart {

    private Browser browser;

    public Cart(Browser browser) {
        this.browser = browser;
    }

    public String getInventoryItemName() {
        return browser.getText(INVENTORY_ITEM_NAME);
    }

    public void checkOutYourInformation() {
        browser.click(CHECKOUT);
    }

}