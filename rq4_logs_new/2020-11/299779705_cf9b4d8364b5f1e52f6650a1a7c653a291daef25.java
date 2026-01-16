package com.example.cmput301f20t18;

import android.app.Activity;
import android.widget.EditText;

import androidx.test.platform.app.InstrumentationRegistry;
import androidx.test.rule.ActivityTestRule;

import com.robotium.solo.Solo;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class LoginActivityTest {
    private Solo solo;
    @Rule
    public ActivityTestRule<Login> rule =
            new ActivityTestRule<>(Login.class, true, true);
    @Before
    public void setUp() throws Exception{
        solo = new Solo(InstrumentationRegistry.getInstrumentation(), rule.getActivity());
    }
    @Test
    public void start(){
        Activity activity = rule.getActivity();
    }
    @Test
    public void Login(){
        solo.assertCurrentActivity("Wrong Activity - NOT LOGIN", Login.class);
        solo.enterText((EditText)solo.getView(R.id.username), "RegisterBot@RegisterBot.bot");
        solo.enterText((EditText)solo.getView(R.id.password), "RoboPass");

        solo.clickOnButton("Sign in");

        solo.assertCurrentActivity("Wrong Activity - NOT HOMESCREEN", HomeScreen.class);
    }
}