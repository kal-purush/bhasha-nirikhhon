package ui_beambox_automation;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Sitemap {
    private static final String BASE_URL = "https://beambox.com/";
    private static final String LOCAL_BASE_URL = "http://lvh.me:3000/";
    private WebDriver driver;

    public Sitemap(WebDriver driver) {
        this.driver = driver;
    }

    public WebDriver getDriver() {
        return driver;
    }

    public void setDriver(WebDriver driver) {
        this.driver = driver;
    }

    public void sitemap() throws InterruptedException {

    List<String> webSiteMapUrls = countUrlsInDiv();
    List<String> siteMapXMLUrls = readAndCountUrlsInSitemap();

    System.out.println("Web Site Page Url Count: "+ webSiteMapUrls.size());
    System.out.println("XML File SiteMap Urls Count"+ siteMapXMLUrls.size());

    List<String> splittingWebSiteUrls = splitUrls(webSiteMapUrls, LOCAL_BASE_URL);
    List<String> splittingXMLSiteUrls = splitUrls(siteMapXMLUrls, BASE_URL);

    System.out.println("After Spliting the Web Site Page Url Count: "+ splittingWebSiteUrls.size());
    System.out.println("After Spliting the XML File SiteMap Urls Count: "+ splittingXMLSiteUrls.size());


    //List<String> afterComparingTheUrls = compareTheUrls(splittingWebSiteUrls, splittingXMLSiteUrls);
    List<String> afterComparingTheUrls = compareTheUrls(splittingXMLSiteUrls, splittingWebSiteUrls);
    System.out.println("After Compering the SiteMap Urls Count: "+ afterComparingTheUrls.size());
    


    //printUrls(afterComparingTheUrls);

    driver.quit();
}

private List<String> countUrlsInDiv() throws InterruptedException {
    driver.get("http://lvh.me:3000/sitemap/");
    driver.navigate().refresh();
    driver.manage().window().maximize();
    Thread.sleep(2000);

    WebElement divElement = driver.findElement(By.xpath("/html/body/div[3]/div"));
    List<WebElement> urls = divElement.findElements(By.tagName("a"));

    List<String> urlStrings = new ArrayList<>();
    for (WebElement url : urls) {
        urlStrings.add(url.getAttribute("href"));
    }

    return urlStrings;
}

private List<String> readAndCountUrlsInSitemap() {
    List<String> sitemapUrls = new ArrayList<>();

    try {
        URL url = new URL("https://beambox.com/sitemap.xml");
        URLConnection connection = url.openConnection();
        BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
            Matcher matcher = Pattern.compile("<loc>(.*?)</loc>").matcher(line);
            while (matcher.find()) {
                sitemapUrls.add(matcher.group(1));
            }
        }
        reader.close();
        System.out.println("Actual Number of URLs in the Sitemap XML : " + sitemapUrls.size());

        

    } catch (IOException e) {
        e.printStackTrace();
    }
    return sitemapUrls;
}


public static List<String> splitUrls(List<String> siteMapUrls, String BASE_URL) {
    List<String> splitUrls = new ArrayList<>();
    for (String url : siteMapUrls) {
        if (url.startsWith(BASE_URL)) {
            String[] parts = url.split(BASE_URL);
            if (parts.length > 1) {
                splitUrls.add(parts[1]);
            }
        }
    }
    return splitUrls;
}

private List<String> compareTheUrls(List<String> arrayList1, List<String> arrayList2) {
    arrayList1.removeAll(arrayList2);
     return arrayList1;

}

private void printUrls(List<String> urls){
    for (String url : urls) {
        System.out.println(url);
    }
}

private List<String> reverseTheSpliting(List<String> reverseUrls, String BASE_URL){
    List<String> reverseUrlsBaseURL = new ArrayList<>();
    for(String reverseUrl : reverseUrls) {
        reverseUrlsBaseURL.add(BASE_URL+reverseUrl);
    }

    return reverseUrlsBaseURL;
}

}