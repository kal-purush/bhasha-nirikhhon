package com.example.cmput301f20t18;

import android.view.View;
import android.widget.EditText;

import androidx.test.platform.app.InstrumentationRegistry;
import androidx.test.rule.ActivityTestRule;

import com.robotium.solo.Solo;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class CheckProfileActivityTest {
    private Solo solo;

    public static final String EMAIL = "OtherBot@OtherBot.botnet";
    public static final String PASSWORD = "BotPass";

    @Rule
    public ActivityTestRule<Login> rule =
            new ActivityTestRule<>(Login.class, true, true);
    @Before
    public void setUp() throws Exception{
        solo = new Solo(InstrumentationRegistry.getInstrumentation(), rule.getActivity());
        LoginActivityTest.Login(solo, EMAIL, PASSWORD);

        solo.assertCurrentActivity("Wrong Activity - NOT HOMESCREEN", HomeScreen.class);
        solo.clickOnView(solo.getView(R.id.tab_search));
        solo.clickOnView(solo.getView(R.id.search_spinner));
        solo.clickOnText("Users");

        EditText searchEditText = (EditText)solo.getView(R.id.search_edit_text);
        View searchButton = solo.getView(R.id.search_button);


        solo.enterText(searchEditText, RegisterActivityTest.DEFAULT_USERNAME);
        solo.clickOnView(searchButton);
        solo.clearEditText(searchEditText);

        solo.clickOnButton("View Profile");
    }
    @Test
    public void start(){
        solo.assertCurrentActivity("Wrong Activity - NOT CHECKPROFILEACTIVITY",
                CheckProfileActivity.class);
    }
    @Test
    public void checkData(){
        solo.assertCurrentActivity("Wrong Activity - NOT CHECKPROFILEACTIVITY",
                CheckProfileActivity.class);
        assertTrue(solo.waitForText(RegisterActivityTest.DEFAULT_USERNAME, 1, 2000));
        assertTrue(solo.waitForText(RegisterActivityTest.DEFAULT_PHONE, 1, 2000));
        assertTrue(solo.waitForText(RegisterActivityTest.DEFAULT_EMAIL, 1, 2000));
    }
}