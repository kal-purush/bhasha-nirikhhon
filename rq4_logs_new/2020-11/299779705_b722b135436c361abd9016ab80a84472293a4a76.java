package com.example.cmput301f20t18;

import android.app.Activity;
import android.widget.EditText;

import androidx.test.platform.app.InstrumentationRegistry;
import androidx.test.rule.ActivityTestRule;

import com.robotium.solo.Solo;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class ScannerTest {

    public static final String DEMO_BOOK_TITLE = "Vagabond";
    public static final String DEMO_AUTHOR = "Takehiko Inoue, Eiji Yoshikawa";
    public static final String DEMO_YEAR = "2002";
    public static final String DEMO_ISBN = "9781421520544";


    private Solo solo;
    @Rule
    public ActivityTestRule<Scanner> rule =
            new ActivityTestRule<>(Scanner.class, true, true);
    @Before
    public void setUp() throws Exception{
        solo = new Solo(InstrumentationRegistry.getInstrumentation(), rule.getActivity());
        LoginActivityTest.Login(solo);
        solo.clickOnView(solo.getView(R.id.tab_scan));
    }
    @Test
    public void start(){
        Activity activity = rule.getActivity();
    }

    @Test
    public void scan(){
        solo.assertCurrentActivity("Wrong Activity - NOT SCANNER",
                Scanner.class);
        solo.clickOnButton(R.id.button_manual);
        solo.enterText((EditText)solo.getView(R.id.manual_isbn_input), DEMO_ISBN);
        solo.clickOnButton("GO");
        solo.clearEditText((EditText) solo.getView(R.id.manual_isbn_input));
        solo.assertCurrentActivity("Wrong Activity - NOT POST SCAN ACTIVITY",
                PostScanActivity.class);
        solo.clickOnButton(R.id.back);
    }

    @Test
    public void scanAddBookCancel(){
        solo.assertCurrentActivity("Wrong Activity - NOT SCANNER",
                Scanner.class);
        solo.clickOnButton(R.id.button_manual);
        solo.enterText((EditText)solo.getView(R.id.manual_isbn_input), DEMO_ISBN);
        solo.clickOnButton("GO");
        solo.clearEditText((EditText) solo.getView(R.id.manual_isbn_input));
        solo.assertCurrentActivity("Wrong Activity - NOT POST SCAN ACTIVITY",
                PostScanActivity.class);
        assertTrue(solo.waitForText(DEMO_ISBN, 1, 2000));
        solo.clickOnButton(R.id.done_add_book);
        solo.assertCurrentActivity("Wrong Activity - NOT POST SCAN ACTIVITY",
                MyBooksAddBook.class);
        solo.clickOnButton(R.id.return_to_my_books);
        solo.assertCurrentActivity("Wrong Activity - NOT POST SCAN ACTIVITY",
                MyBooksFragment.class);
        assertFalse(solo.waitForText(DEMO_BOOK_TITLE, 1, 2000));
        assertFalse(solo.waitForText(DEMO_AUTHOR, 1, 2000));
        assertFalse(solo.waitForText(DEMO_YEAR, 1, 2000));
        assertFalse(solo.waitForText(DEMO_ISBN, 1, 2000));

    }

    @Test
    public void scanAddBookDone(){

        ScannerTest.addBook(solo);
        MyBooksAvailableFragmentTest.deleteAll(solo);
    }

    @Test
    public void searchISBN () {


        ScannerTest.addBook(solo);
        solo.clickOnView(solo.getView(R.id.tab_scan));
        solo.assertCurrentActivity("Wrong Activity - NOT SCANNER",
                Scanner.class);
        solo.clickOnButton(R.id.button_manual);
        solo.enterText((EditText)solo.getView(R.id.manual_isbn_input), DEMO_ISBN);
        solo.clickOnButton("GO");
        solo.clearEditText((EditText) solo.getView(R.id.manual_isbn_input));
        solo.assertCurrentActivity("Wrong Activity - NOT POST SCAN ACTIVITY",
                PostScanActivity.class);
        assertTrue(solo.waitForText(DEMO_ISBN, 1, 2000));
        solo.clickOnButton(R.id.search_copies);
        solo.assertCurrentActivity("Wrong Activity - SEARCH FRAGMENT",
                SearchFragment.class);
        assertTrue(solo.waitForText(DEMO_ISBN, 1, 2000));
        solo.clickOnButton(R.id.search_button);
        assertTrue(solo.waitForText(DEMO_BOOK_TITLE, 1, 2000));
        assertTrue(solo.waitForText(DEMO_AUTHOR, 1, 2000));
        assertTrue(solo.waitForText(DEMO_YEAR, 1, 2000));
        assertTrue(solo.waitForText(DEMO_ISBN, 1, 2000));

        MyBooksAvailableFragmentTest.deleteAll(solo);

    }

    private static void addBook (Solo solo){

        solo.assertCurrentActivity("Wrong Activity - NOT SCANNER",
                Scanner.class);
        solo.clickOnButton(R.id.button_manual);
        solo.enterText((EditText)solo.getView(R.id.manual_isbn_input), DEMO_ISBN);
        solo.clickOnButton("GO");
        solo.clearEditText((EditText) solo.getView(R.id.manual_isbn_input));
        solo.assertCurrentActivity("Wrong Activity - NOT POST SCAN ACTIVITY",
                PostScanActivity.class);
        assertTrue(solo.waitForText(DEMO_ISBN, 1, 2000));
        solo.clickOnButton(R.id.add_book_isbn);
        solo.assertCurrentActivity("Wrong Activity - MY BOOKS ADD BOOK",
                MyBooksAddBook.class);
        solo.clickOnButton(R.id.done_add_book);
        assertTrue(solo.waitForText(DEMO_BOOK_TITLE, 1, 2000));
        assertTrue(solo.waitForText(DEMO_AUTHOR, 1, 2000));
        assertTrue(solo.waitForText(DEMO_YEAR, 1, 2000));
        assertTrue(solo.waitForText(DEMO_ISBN, 1, 2000));

    }


}