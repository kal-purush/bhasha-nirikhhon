import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.testng.Assert;
import org.testng.annotations.Test;

public class Homework17 extends BaseTest {



    @Test
    public void addSongToPlaylist(){
        //login
        provideEmail("demo@class.com");
        providePassword();
        clickSubmitBtn();
        //add a song to the playlist
        clickViewAllBtn();
        String getSongTitle = getSongTitleTxt();;
        clickSongFromList();
        clickAddToPlaylist();
        addToSuperPlaylist();
        clickOnSuperPlaylist();

        //Verify the song
        String songFromSuperPlaylist = getSongTitleFromSuperPlaylist();
        Assert.assertEquals(songFromSuperPlaylist, getSongTitle);
    }

    private String getSongTitleFromSuperPlaylist() {

        WebElement superPlaylistSong = driver.findElement(By.xpath("//section[@id='playlistWrapper']//tr[@class='song-item']//td[@class='title']"));
        return superPlaylistSong.getText();
    }

    private void clickOnSuperPlaylist() {
        WebElement superPlaylistLink = driver.findElement(By.xpath("//a[text()='super']"));
        superPlaylistLink.click();
    }

    private void addToSuperPlaylist() {
        WebElement superPlaylist = driver.findElement(By.xpath("//section[@id='recentlyPlayedWrapper']//li[contains(text(), 'super')]"));
        superPlaylist.click();
    }

    private String getSongTitleTxt() {
        WebElement getTitleTxt = driver.findElement(By.xpath("//section[@id='recentlyPlayedWrapper']//tr[@class='song-item']//td[@class='title']"));
        return getTitleTxt.getText();
    }

    private void clickAddToPlaylist() {
        WebElement addToPlaylist = driver.findElement(By.xpath("//button[contains(@title, 'Add selected songs')]"));
        addToPlaylist.click();
    }

    private void clickSongFromList() {
        WebElement songList = driver.findElement(By.xpath("//section[@id='recentlyPlayedWrapper']//tr[@class='song-item']"));
        songList.click();

    }

    private void clickViewAllBtn() {
        WebElement viewAllBtn = driver.findElement(By.xpath("//button[@data-testid='home-view-all-recently-played-btn']"));
        viewAllBtn.click();
    }
}