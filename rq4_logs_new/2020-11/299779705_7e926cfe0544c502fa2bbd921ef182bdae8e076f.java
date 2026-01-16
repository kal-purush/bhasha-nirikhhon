package com.example.cmput301f20t18;

import android.app.Activity;
import androidx.test.ext.junit.runners.AndroidJUnit4;
import androidx.test.platform.app.InstrumentationRegistry;
import androidx.test.rule.ActivityTestRule;
import android.widget.EditText;
import android.widget.ListView;
import com.robotium.solo.Solo;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test class for Login/Registration Activity. All the UI tests are written here. Robotium test framework is
 used
 */
@RunWith(AndroidJUnit4.class)

public class LoginRegistrationUITest {

    private Solo solo;
    @Rule
    public ActivityTestRule<Login> rule =
            new ActivityTestRule<>(Login.class, true, true);
    /**
     * Runs before all tests and creates solo instance.
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception{
        solo = new Solo(InstrumentationRegistry.getInstrumentation(),rule.getActivity());

    }
    /**
     * Gets the Activity
     * @throws Exception
     */
    @Test
    public void start() throws Exception{
        Activity activity = rule.getActivity();
    }

    /**
     * Registers a new user into the database
     */
    @Test
    public void RegisterUser(){
        solo.assertCurrentActivity("Wrong Activity", Login.class);
        solo.clickOnButton("Register"); //Click Register Button
        //solo.sleep(60000);
        solo.assertCurrentActivity("Wrong Activity", Register.class);
        solo.enterText((EditText) solo.getView(R.id.username), "JohnathonGil");
        solo.enterText((EditText) solo.getView(R.id.password), "Comp@Wiz2020");
        solo.enterText((EditText) solo.getView(R.id.email), "johnathongil12@hotmail.com");
        solo.enterText((EditText) solo.getView(R.id.address), "21 Cherry Crescent, St.Albert");
        solo.clickOnButton("Sign Up"); //Select Sign Up Button
        solo.assertCurrentActivity("Wrong Activity", HomeScreen.class);
    }

    @Test

    public void LoginUser(){

        solo.assertCurrentActivity("Wrong Activity", Login.class);
        solo.enterText((EditText) solo.getView(R.id.username), "johnathongil12@hotmail.com");
        solo.enterText((EditText) solo.getView(R.id.password), "Comp@Wiz2020");
        solo.clickOnButton("Sign In"); //Click Register Button
        solo.assertCurrentActivity("Wrong Activity", HomeScreen.class);

    }

    /**
     * Close activity after each test
     * @throws Exception
     */
    @After
    public void tearDown() throws Exception{
        solo.finishOpenedActivities();
    }

}