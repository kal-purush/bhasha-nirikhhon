package test.java.Artefacts;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import test.java.Artefacts.pages.NotebooksPage;

import static org.testng.Assert.assertEquals;

public class TestNotebooks extends TestBaseSetup {
    NotebooksPage notebooksPage;

    @BeforeMethod
    public void initialize() {
        notebooksPage = new NotebooksPage(driver);
    }

    @Test
    public void testFilter() {
        String searchStr = "Acer";
        notebooksPage.open();
        notebooksPage.chooseProducer(searchStr);
        int actualNumberElements = notebooksPage.getElements().size();
        int expectedNumberElements = notebooksPage.getElements(searchStr).size();
        assertEquals(
                actualNumberElements,
                expectedNumberElements,
                "Number of the items to be " + expectedNumberElements + " but got " + actualNumberElements);
    }
}