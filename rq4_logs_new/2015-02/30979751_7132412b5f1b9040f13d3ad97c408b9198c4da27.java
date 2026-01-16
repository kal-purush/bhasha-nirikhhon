import com.codeborne.selenide.Configuration;
import com.codeborne.selenide.WebDriverRunner;
import helpers.CustomListener;
import net.lightbody.bmp.core.har.Har;
import net.lightbody.bmp.core.har.HarEntry;
import net.lightbody.bmp.core.har.HarRequest;
import net.lightbody.bmp.core.har.HarResponse;
import net.lightbody.bmp.proxy.ProxyServer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import pages.stat.SlowResourcesPage;
import pages.stat.StartPage;

import static com.codeborne.selenide.Selenide.open;
import static com.codeborne.selenide.Selenide.sleep;
import static com.codeborne.selenide.WebDriverRunner.getWebDriver;
import static org.openqa.selenium.net.PortProber.findFreePort;

@Listeners(CustomListener.class)
public class ResponseCodeTest {
    ProxyServer proxyServer;
    Har har;

    @BeforeMethod
    public void setup() throws Exception {
        Configuration.browser = "chrome";
        Configuration.baseUrl = "http://the-internet.herokuapp.com";
        Configuration.timeout = 10000;
        proxyServer = new ProxyServer(findFreePort());
        proxyServer.start();
        WebDriverRunner.setProxy(proxyServer.seleniumProxy());
        open("/");
    }

    @Test
    public void responseCodeTest() throws Exception {
        proxyServer.newHar("slowResources");
        StartPage.open("Slow Resources");
        Assert.assertTrue(SlowResourcesPage
                .isSlowResourcesPage(), "Failed to open Slow Resources page");

        har = proxyServer.getHar();

        for(HarEntry entry : har.getLog().getEntries()) {
            HarRequest request = entry.getRequest();
            HarResponse response = entry.getResponse();

            sleep(30000);

            Assert.assertEquals(response.getStatus(), 200,
                    "URL: " + request.getUrl() + " has bad response status");
        }
    }

    @AfterMethod
    public void stopBrowserMobProxyServer() throws Exception {
        WebDriverRunner.setProxy(null);
        proxyServer.stop();
        getWebDriver().quit();
    }
}