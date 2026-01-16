package com.example.cmput301f20t18;

import com.robotium.solo.Solo;
public class RobotiumTransactionManager {
    public static Boolean requestBook(Solo solo, String bookTitle, String searchKey){
        final String TITLE = RobotiumUserBookManager.DEFAULT_BOOK_TITLE;
        solo.assertCurrentActivity("Wrong Activity - NOT HOMESCREEN", HomeScreen.class);
        solo.clickOnView(solo.getView(R.id.tab_search));
        RobotiumSearchManager.searchBook(solo, TITLE, TITLE);
        solo.clickOnButton("Request Book");
        return solo.waitForText("Cancel Request");
    }
}