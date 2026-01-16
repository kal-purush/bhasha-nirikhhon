package com.example.cmput301f20t18;

import android.widget.EditText;

import com.robotium.solo.Solo;

/**
 * Handles logging in as various robotium controlled users
 */
public class RobotiumLoginManager {
    public static final RobotiumUser owner = new RobotiumUser("OwnerBot",
            "OwnerBot@OwnerBot.botnet", "Edmonton AB");
    public static final RobotiumUser borrower = new RobotiumUser("BorrowerBot",
            "BorrowerBot@BorrowerBot.botnet", "Calgary AB");


    public static void loginOwner(Solo solo){
        solo.assertCurrentActivity("Wrong Activity - NOT LOGIN", Login.class);
        solo.enterText((EditText)solo.getView(R.id.username), owner.getEmail());
        solo.enterText((EditText)solo.getView(R.id.password), owner.getPassword());
        solo.clickOnButton("Sign in");

    }
    public static void loginBorrower(Solo solo){
        solo.assertCurrentActivity("Wrong Activity - NOT LOGIN", Login.class);
        solo.enterText((EditText)solo.getView(R.id.username), borrower.getEmail());
        solo.enterText((EditText)solo.getView(R.id.password), borrower.getPassword());
        solo.clickOnButton("Sign in");
    }

    public static void signOut(Solo solo){
        solo.assertCurrentActivity("Wrong Activity - NOT HOMESCREEN", HomeScreen.class);
        solo.clickOnView(solo.getView(R.id.tab_profile));
        solo.clickOnButton("Sign Out");
        solo.assertCurrentActivity("Wrong Activity - NOT LOGIN", Login.class);
    }
}