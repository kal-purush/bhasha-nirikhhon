package demo;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
//import org.openqa.selenium.firefox.FirefoxDriver;
//import org.openqa.selenium.edge.EdgeDriver;
//import org.openqa.selenium.ie.InternetExplorerDriver;
//import org.openqa.selenium.opera.OperaDriver;
//import org.openqa.selenium.phantomjs.PhantomJSDriver;
import org.openqa.selenium.chrome.ChromeDriver;

/**
 * @Description: 通过selenium操作浏览器打开百度进行搜索
 * selenium版本：3.12.0； 通过maven管理jar包
 * 开发工具：IDEA
 * jdk：1.8
 * 浏览器：chrome
 */
public class Test01 {
    public static void main(String[] args) {
        // 1.创建webdriver驱动

//        System.setProperty ( "webdriver.chrome.bin" , "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe" );
//        System.setProperty ( "webdriver.chrome.driver" , "D:\\webdriver\\chromedriver.exe" );

//        WebDriver driver = new FirefoxDriver();   //Firefox浏览器
        WebDriver driver = new ChromeDriver();

        // 2.打开百度首页
        driver.get("https://www.baidu.com");
        // 3.获取输入框，输入selenium
//        driver.findElement(By.id("kw")).sendKeys("selenium");
//        driver.findElement(By.name("wd")).sendKeys("Java");

        // 4.获取“百度一下”按钮，进行搜索
//        driver.findElement(By.id("su")).click();
        //        操作一个新闻连接，by link
//        driver.findElement(By.linkText("新闻")).click();
//        xpath 相对路径：//input[@属性='值']
        driver.findElement(By.xpath("//input[@id='kw']")).sendKeys("xpath");
        // 5.退出浏览器
//        driver.quit();
    }
}