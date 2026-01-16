import org.junit.*;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.Select;
import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;

public class SeleniumTests{

    final static WebDriver driver = Driver.getChromeDriver();
    final static String URL = "https://www.avito.ru/moskva/transport";

    final static String auto = ".js-search-form-category";
    final static String type_technic = ".select-select-2g7DN";

    final static String search = "#search_form > div.search-form-main-controls.js-search-form-main-controls > div.search-form__row.search-form__row_1.clearfix > div.search-form__submit > input";
    final static String list = "div.description.item_table-description.snippet-experiment-wrapper > div.item_table-header > h3 > a > span";
    final static String date ="[data-absolute-date*='Сегодня']";


    final static String name = ".item-description-title-link";
    final static String price = ".price";
    final static String info = ".specific-params.specific-params_block";

    @BeforeClass
    public static void setup() {
        driver.get(URL);
    }


    @Test
    public void all_baik() {
        WebElement selectElem = driver.findElement(By.cssSelector(auto));
        Select select = new Select(selectElem);
        select.selectByVisibleText("Мотоциклы и мототехника");

        driver.manage().timeouts().implicitlyWait(2,SECONDS);
        WebElement baik = driver.findElement(By.cssSelector(type_technic));
        Select baik_ = new Select(baik);
        baik_.selectByVisibleText("Мотоциклы");

        //нажимаем кнопку "Найти"
        WebElement button_cr = driver.findElement(By.cssSelector(search));
        button_cr.click();

        //ищем и выводим назвнаия всех объявлений
        System.out.println("Названия мотоциклов:");
        List<WebElement> list_baik = driver.findElements(By.cssSelector(list));
        for (WebElement element : list_baik) {
            System.out.println(element.getText());
        }
    }
    @Test
    public void add_today() {
        //выбрать в выпадающем списке автомобили
        WebElement selectElem = driver.findElement(By.cssSelector(auto));
        Select select = new Select(selectElem);
        select.selectByVisibleText("Автомобили");

        WebElement button_cr = driver.findElement(By.cssSelector(search));
        button_cr.click();
        //ищем и выводим назвнаия всех объявлений
        System.out.println("Названия автомобилей, объявления о продаже которых были\n" +
                "добавлены сегодня");

            List<WebElement> list_name = driver.findElements(By.cssSelector(name));
            List<WebElement> list_date = driver.findElements(By.cssSelector(date));

            for (WebElement name : list_name) {
                for (WebElement date : list_date)
                    System.out.println(name.getText() + "\n" + date.getText());
            }

    }
        @Test
        public void audi(){

        WebElement selectElem = driver.findElement(By.cssSelector(auto));
        Select select = new Select(selectElem);
        select.selectByVisibleText("Автомобили");

        driver.manage().timeouts().implicitlyWait(2,SECONDS);
        WebElement audi_ = driver.findElement(By.cssSelector(type_technic));
        Select audi_sel = new Select(audi_);
        audi_sel.selectByVisibleText("Audi");

        //нажимаем кнопку "Найти"
        WebElement button_cr = driver.findElement(By.cssSelector(search));
        button_cr.click();

        //выводим название 1го ауди
        WebElement first_audi = driver.findElement(By.cssSelector(name));
        WebElement audi_price = driver.findElement(By.cssSelector(price));
        WebElement audi_info = driver.findElement(By.cssSelector(info));
        System.out.println("Первая audi в поиске:");
        System.out.println(first_audi.getText() + "\n" + audi_price.getText() + "\n" + audi_info.getText());

    }

    @AfterClass
    public static void killDriver(){
        driver.close();
    }

}