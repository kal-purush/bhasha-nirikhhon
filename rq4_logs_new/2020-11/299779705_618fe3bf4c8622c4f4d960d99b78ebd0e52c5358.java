package com.example.cmput301f20t18;

import android.app.Activity;

import androidx.test.platform.app.InstrumentationRegistry;
import androidx.test.rule.ActivityTestRule;

import com.robotium.solo.Solo;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class BorrowerFragmentTest {

    public static final String DEFAULT_BOOK_TITLE = "Robots Can't Read!";
    public static final String DEFAULT_BOOK_AUTHOR = "AuthorBot9000";
    public static final String DEFAULT_BOOK_YEAR = "2020";
    public static final String DEFAULT_BOOK_ISBN = "1001001101110";

    private Solo solo;
    @Rule
    public ActivityTestRule<Login> rule =
            new ActivityTestRule<>(Login.class, true, true);
    @Before
    public void setUp() throws Exception{
        solo = new Solo(InstrumentationRegistry.getInstrumentation(), rule.getActivity());
        RobotiumLoginManager.loginOwner(solo);
        solo.clickOnView(solo.getView(R.id.tab_borrowed));

    }

    @Test
    public void start(){
        Activity activity = rule.getActivity();
    }

    @Test
    public void checkRequestedBook () {

        solo.assertCurrentActivity("Wrong Activity - HOMESCREEN",
                HomeScreen.class);
        RobotiumUserBookManager.add(solo);
        solo.clickOnView(solo.getView(R.id.tab_profile));
        solo.clickOnButton("Sign Out");
        RobotiumLoginManager.loginBorrower(solo);
        RobotiumTransactionManager.requestBook(solo,DEFAULT_BOOK_TITLE,DEFAULT_BOOK_TITLE);
        solo.clickOnView(solo.getView(R.id.tab_borrowed));
        assertTrue(solo.waitForText(RobotiumUserBookManager.DEFAULT_BOOK_TITLE,
                1, 2000));
        assertTrue(solo.waitForText(RobotiumUserBookManager.DEFAULT_BOOK_AUTHOR,
                1, 2000));
        assertTrue(solo.waitForText(RobotiumUserBookManager.DEFAULT_BOOK_YEAR,
                1, 2000));
        assertTrue(solo.waitForText(RobotiumUserBookManager.DEFAULT_BOOK_ISBN,
                1, 2000));
    }

    @Test
    public void checkRequestingBooks() {

        solo.assertCurrentActivity("Wrong Activity - HOMESCREEN",
                HomeScreen.class);
        RobotiumUserBookManager.add(solo);
        solo.clickOnView(solo.getView(R.id.tab_profile));
        solo.clickOnButton("Sign Out");
        RobotiumLoginManager.loginBorrower(solo);
        RobotiumTransactionManager.requestBook(solo,DEFAULT_BOOK_TITLE,DEFAULT_BOOK_TITLE);
        solo.clickOnView(solo.getView(R.id.tab_profile));
        solo.clickOnButton("Sign Out");
        RobotiumLoginManager.loginOwner(solo);
        solo.clickOnView(solo.getView(R.id.tab_borrowed));
        solo.clickOnView(solo.getView(R.id.tab_borrowed_borrowing));
    }
}



/*solo.clickOnView(solo.getView(R.id.tab_profile));
        solo.clickOnButton("Sign Out");
        RobotiumLoginManager.loginOwner(solo);*/