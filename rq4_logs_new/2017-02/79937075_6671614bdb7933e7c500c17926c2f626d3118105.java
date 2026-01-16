package org.naturenet;

import android.support.test.rule.ActivityTestRule;
import android.support.test.runner.AndroidJUnit4;
import static org.junit.Assert.assertTrue;
import static android.support.test.espresso.Espresso.onView;
import static android.support.test.espresso.action.ViewActions.click;
import static android.support.test.espresso.action.ViewActions.closeSoftKeyboard;
import static android.support.test.espresso.matcher.ViewMatchers.withId;
import static android.support.test.espresso.action.ViewActions.typeText;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Created by rigot on 1/30/2017.
 */

@RunWith(AndroidJUnit4.class)
public class LoginTest {

    String username;
    String password;


    @Before
    public void setup(){
        //initialize string variables
        username = "rtrejo@uncc.edu";
        password = "777777";

    }

    @Test
    public void login_test(){
        //type in username/email
        onView(withId(R.id.login_et_email_address)).perform(typeText(username), closeSoftKeyboard());

        //type in password
        onView(withId(R.id.login_et_email_address)).perform(typeText(password), closeSoftKeyboard());

        //click login button
        onView(withId(R.id.login_b_sign_in)).perform(click());

        //verify success
        //assertTrue();
    }

}