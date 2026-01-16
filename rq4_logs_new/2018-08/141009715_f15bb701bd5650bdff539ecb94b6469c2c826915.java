import org.junit.*;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SeleniumTests{

    final static WebDriver driver = Driver.getChromeDriver();
    final static String URL = "https://www.survio.com/ru/";
    final static String INPUT_NAME = "/html/body/section[1]/div/form/div[1]/input";
    final static String INPUT_EMAIL = "//*[@id=\"reg_email\"]";
    final static String INPUT_PAS = "/html/body/section[1]/div/form/div[3]/input";
    final static String CREATE = "/html/body/section[1]/div/form/div[4]/a";
    final static String MENU = " #menu > div > h1";
    final static String INPUT_EMAIL_ERROR = "#reg_email-error";

    @BeforeClass
    public static void setup() {
        driver.get(URL);
    }

    @Test
    public void login_positive(){

        WebElement name = driver.findElement(By.xpath(INPUT_NAME));
        name.sendKeys("Галия");
        WebElement email = driver.findElement(By.xpath(INPUT_EMAIL));
        //очищаем поле emal
        email.clear();
        email.sendKeys("xalgalrin@gmail.com");
        WebElement pas = driver.findElement(By.xpath(INPUT_PAS));
        pas.sendKeys("gfnhbr18");
        WebElement button_cr = driver.findElement(By.xpath(CREATE));
        button_cr.click();

        //ждем загрузку 10 сек
        driver.manage().timeouts().implicitlyWait(10,SECONDS);
        WebElement create_survio = driver.findElement(By.cssSelector(MENU));
        String question = create_survio.getText();
        //считаем, что если появилась строка с содержанием ниже, то мы успешно прошли регистрацию
        Assert.assertEquals("Каким способом Вы хотите создать опрос?", question);
        System.out.println("Позитивный тест пройден. Успешная регистрация опроса\n");
    }

    @Test
    public void login_negative(){

        driver.get(URL);
        WebElement email = driver.findElement(By.xpath(INPUT_EMAIL));
        email.clear();
        WebElement button_create = driver.findElement(By.xpath(CREATE));
        button_create.click();

        WebElement email_err = driver.findElement(By.cssSelector(INPUT_EMAIL_ERROR));
        String em_err = email_err.getText();

        //считаем, что если появилась строка с содержанием ниже, то мы успешно прошли негативный тест на проверку пустого поля
        Assert.assertEquals("Это поле необходимо заполнить.", em_err);
        System.out.println("Негативный тест пройден. Не заполнены обязательные поля, поэтому регистрация не прошла\n");
    }

    @AfterClass
    public static void killDriver(){
        driver.close();
    }

}