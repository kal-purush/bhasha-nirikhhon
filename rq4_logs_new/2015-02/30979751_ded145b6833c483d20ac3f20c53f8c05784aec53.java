package core;

import com.codeborne.selenide.Configuration;
import com.codeborne.selenide.WebDriverRunner;
import net.lightbody.bmp.core.har.Har;
import net.lightbody.bmp.proxy.ProxyServer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import static com.codeborne.selenide.Selenide.open;
import static com.codeborne.selenide.WebDriverRunner.getWebDriver;
import static org.openqa.selenium.net.PortProber.findFreePort;

public class ProxyTestBase {
    protected ProxyServer proxyServer;
    protected Har har;

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


    @AfterMethod
    public void stopBrowserMobProxyServer() throws Exception {
        WebDriverRunner.setProxy(null);
        proxyServer.stop();
        getWebDriver().quit();
    }
}