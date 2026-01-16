import android.support.test.filters.LargeTest;
import android.support.test.rule.ActivityTestRule;
import android.support.test.runner.AndroidJUnit4;

import com.example.chriscu.visualapp.MainActivity;
import com.example.chriscu.visualapp.R;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import static android.support.test.espresso.Espresso.onView;
import static android.support.test.espresso.assertion.ViewAssertions.matches;
import static android.support.test.espresso.matcher.ViewMatchers.withId;
import static android.support.test.espresso.matcher.ViewMatchers.withText;

/**
 * Created by chriscurtis on 13/02/2017.
 */
////https://developer.android.com/training/testing/unit-testing/local-unit-tests.html#build

////http://wptrafficanalyzer.in/blog/android-testing-example-helloworld-with-activityunittestcase/

//http://www.vogella.com/tutorials/AndroidTesting/article.html#androidsystem_tests

///https://developer.android.com/training/testing/start/index.html#config-instrumented-tests

    @RunWith(AndroidJUnit4.class)
    @LargeTest
    public class UITest {

        @Rule
        public ActivityTestRule mActivityRule = new ActivityTestRule<>(
                MainActivity.class);


        @Test
        public void VegasButton(){

            onView(withId(R.id.button)).check(matches(withText("VEGAS")));
            System.out.println("checking Vegas Button named correctly");
            System.out.println("Vegas Button IS named correctly");
        }


    @Test
    public void SportsButton(){
        System.out.println("checking Sports Button named correctly");
        onView(withId(R.id.button4)).check(matches(withText("SPORTS")));
        System.out.println("Sports Button IS named correctly");
    }


    @Test
    public void PromosButton() {
        System.out.println("checking Promo Button named correctly");
        onView(withId(R.id.button2)).check(matches(withText("PROMOTIONS")));
        System.out.println("Promo Button IS named correctly");
    }}


