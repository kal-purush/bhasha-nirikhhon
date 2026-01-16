import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.testng.Assert;
import org.testng.annotations.Test;

public class Homework19 extends BaseTest {

    @Test
    public void deletePlaylist() throws InterruptedException {
        // Create a test case deletePlaylist() using @Test annotations
        // Use the reusable methods
        // Use @Parameters for passing baseUrl
        // Given: Navigate to "https://bbb.testpro.io/"
        // Step1: Log in with your credentials
        logIn();
        // Step2: Choose a playlist
        selectPlaylist();
        // Step3: Delete the playlist
        pressDeletePlaylist();
        // Step4: Validate that the playlist is deleted "Deleted playlist"
        Assert.assertTrue(somethingToAssert);
    }

    private void selectPlaylist() throws InterruptedException {
        WebElement myPlaylist = driver.findElement(By.cssSelector(".playlist:nth-child(3)"));
        myPlaylist.click();
        Thread.sleep(5000);
    }

    private void pressDeletePlaylist() throws InterruptedException {
        WebElement delPlaylist = driver.findElement(By.cssSelector(".btn-delete-playlist"));
        delPlaylist.click();
        Thread.sleep(4000);
    }



}