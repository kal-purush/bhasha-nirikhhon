package com.example.cmput301f20t18;

import android.widget.EditText;

import androidx.test.platform.app.InstrumentationRegistry;
import androidx.test.rule.ActivityTestRule;

import com.robotium.solo.Solo;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class EditProfileActivityTest {
    private final String USERNAME = "NOT_A_BOT!";
    private final String PHONE = "1234567890";

    private Solo solo;
    @Rule
    public ActivityTestRule<Login> rule =
            new ActivityTestRule<>(Login.class, true, true);
    @Before
    public void setUp() throws Exception {
        solo = new Solo(InstrumentationRegistry.getInstrumentation(), rule.getActivity());
        LoginActivityTest.Login(solo);
        solo.clickOnView(solo.getView(R.id.tab_profile));
        solo.clickOnText("Edit Account");
    }
    @Test
    public void changeDataCancel() {
        solo.assertCurrentActivity("Wrong Activity - NOT EDITPROFILE", EditProfile.class);

        EditText usernameInput = (EditText) solo.getView(R.id.username_input);
        EditText phoneInput = (EditText) solo.getView(R.id.phone_input);
        solo.clearEditText(usernameInput);
        solo.clearEditText(phoneInput);
        solo.enterText(usernameInput, USERNAME);
        solo.enterText(phoneInput, PHONE);

        solo.clickOnView(solo.getView(R.id.return_to_my_profile));

        assertTrue(solo.waitForText(RegisterActivityTest.DEFAULT_USERNAME));
        assertTrue(solo.waitForText(RegisterActivityTest.DEFAULT_PHONE));
    }
    @Test
    public void changeUsername(){
        solo.assertCurrentActivity("Wrong Activity - NOT EDITPROFILE", EditProfile.class);
        EditText usernameInput = (EditText) solo.getView(R.id.username_input);

        solo.clearEditText(usernameInput);
        solo.enterText(usernameInput, USERNAME);
        solo.clickOnButton("Done");
        assertTrue(solo.waitForText(USERNAME));

        solo.clickOnText("Edit Account");
        solo.assertCurrentActivity("Wrong Activity - NOT EDITPROFILE", EditProfile.class);
        usernameInput = (EditText) solo.getView(R.id.username_input);
        solo.clearEditText(usernameInput);
        solo.enterText(usernameInput, RegisterActivityTest.DEFAULT_USERNAME);
        solo.clickOnButton("Done");
        assertTrue(solo.waitForText(RegisterActivityTest.DEFAULT_USERNAME));
    }
    @Test
    public void changePhone(){
        solo.assertCurrentActivity("Wrong Activity - NOT EDITPROFILE", EditProfile.class);

        EditText phoneInput = (EditText) solo.getView(R.id.phone_input);
        solo.clearEditText(phoneInput);
        solo.enterText(phoneInput, PHONE);
        solo.clickOnButton("Done");
        assertTrue(solo.waitForText(PHONE));

        solo.clickOnText("Edit Account");
        phoneInput = (EditText) solo.getView(R.id.phone_input);
        solo.assertCurrentActivity("Wrong Activity - NOT EDITPROFILE", EditProfile.class);
        solo.enterText(phoneInput, RegisterActivityTest.DEFAULT_PHONE);
        solo.clickOnButton("Done");
        assertTrue(solo.waitForText(RegisterActivityTest.DEFAULT_PHONE));
    }
    @Test
    public void changeAddress(){
        solo.assertCurrentActivity("Wrong Activity - NOT EDITPROFILE", EditProfile.class);

        solo.clickOnButton("Select New Address");
        solo.assertCurrentActivity("Wrong Activity - NOT SELECTLOCATIONACTIVITY",
                SelectLocationActivity.class);
    }

}