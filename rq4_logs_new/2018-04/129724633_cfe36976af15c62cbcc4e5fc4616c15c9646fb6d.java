package it.italiancoders.mybudget;

import android.content.Context;
import android.content.Intent;
import android.content.pm.ActivityInfo;
import android.support.test.InstrumentationRegistry;
import android.support.test.espresso.PerformException;
import android.support.test.espresso.action.ViewActions;
import android.support.test.rule.ActivityTestRule;
import android.support.test.runner.AndroidJUnit4;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.runner.RunWith;

import java.io.File;

import it.italiancoders.mybudget.activity.MainActivity_;
import it.italiancoders.mybudget.rest.RestClient;
import it.italiancoders.mybudget.utils.DataUtils;
import it.italiancoders.mybudget.utils.DataUtils_;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;

import static android.support.test.espresso.Espresso.onView;
import static android.support.test.espresso.matcher.ViewMatchers.isRoot;

/**
 * @author fattazzo
 * <p/>
 * date: 20/04/18
 * <p>
 * Java class due to an Android Annotations bug on rule activity generic parameter for generated class
 */
@RunWith(AndroidJUnit4.class)
public class BaseTest {

    private static MockWebServer server;

    protected static DataUtils dataUtils;

    @Rule
    public ActivityTestRule<MainActivity_> mainActivityTestRule = new ActivityTestRule<>(MainActivity_.class, true, false);

    @BeforeClass
    public static void setUp() throws Exception {
        dataUtils = DataUtils_.getInstance_(InstrumentationRegistry.getTargetContext());

        initPrefs();

        System.err.println("setUp");
        server = new MockWebServer();
        server.start();
        RestClient.INSTANCE.setBASE_URL(server.url("/").toString());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        clearPrefs();
        server.shutdown();
    }

    private static void initPrefs() {
        File root = InstrumentationRegistry.getTargetContext().getFilesDir().getParentFile();
        String[] sharedPreferencesFileNames = new File(root, "shared_prefs").list();
        for (String fileName : sharedPreferencesFileNames) {
            InstrumentationRegistry.getTargetContext().getSharedPreferences(fileName.replace(".xml", ""), Context.MODE_PRIVATE).edit()
                    .clear()
                    .putString("lastUserAccounts", "[{\"id\": \"3\",\"name\": \"Other\",\"status\": 0,\"numberOfUsers\": 1},{\"id\": \"2\",\"name\": \"Family\",\"status\": 0,\"numberOfUsers\": 3},{\"id\": \"1\",\"name\": \"Personal\",\"status\": 0,\"numberOfUsers\": 1}]")
                    .putInt("accountType", 0)
                    .putString("lastUser", "{\"alias\":\"qqqq\",\"firstname\":\"aaaa\",\"lastname\":\"aaa\",\"password\":null,\"socialType\":0,\"username\":\"qqqq\"}")
                    .putString("language","IT")
                    .putString("currency","EUR")
                    .commit();
        }
    }

    private static void clearPrefs() {
        File root = InstrumentationRegistry.getTargetContext().getFilesDir().getParentFile();
        String[] sharedPreferencesFileNames = new File(root, "shared_prefs").list();
        for (String fileName : sharedPreferencesFileNames) {
            InstrumentationRegistry.getTargetContext().getSharedPreferences(fileName.replace(".xml", ""), Context.MODE_PRIVATE).edit()
                    .clear()
                    .commit();
        }
    }

    @Before
    public void initActivity() throws Exception {
        enqueueResponse(200, "personalAccountDetails.json");
        mainActivityTestRule.launchActivity(new Intent());

        // Wait splash screen
        Thread.sleep(1000);
    }

    protected void enqueueResponse(int code, String fileName) throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(code)
                .setBody(JsonTestHelper.getStringFromFile(InstrumentationRegistry.getInstrumentation().getContext(), fileName)));
    }



    /**
     * Press on the back button.
     *
     * @throws PerformException if currently displayed activity is root activity, since pressing back
     *                          button would result in application closing.
     */
    public void pressBack() throws PerformException {
        onView(isRoot()).perform(ViewActions.pressBack());
    }

    public void rotatePortrait() {
        mainActivityTestRule.getActivity().setRequestedOrientation(ActivityInfo.SCREEN_ORIENTATION_PORTRAIT);
    }

    public void rotateLandscape() {
        mainActivityTestRule.getActivity().setRequestedOrientation(ActivityInfo.SCREEN_ORIENTATION_LANDSCAPE);
    }
}