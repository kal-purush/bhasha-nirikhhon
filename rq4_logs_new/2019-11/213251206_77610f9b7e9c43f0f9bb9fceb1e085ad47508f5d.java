package przyklad1;
import org.openqa.selenium.chrome.ChromeDriver; // imporowanie rzeczy z różnych bibliotek
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.By;
import org.junit.*;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
public class MyStoreShoppingCart {

    private WebDriver driver;

    @Before
    public void setUp() {
        System.setProperty("webdriver.chrome.driver",
                "src/main/resources/chromedriver");
        // Uruchom nowy egzemplarz przeglądarki Chrome
        driver = new ChromeDriver();

        // Zmaksymalizuj okno przeglądarki
        driver.manage().window().maximize();
        //otworzenie witryny www z formularzem
        driver.get("https://prod-kurs.coderslab.pl/index.php");
    }

    @Test
    public void myStoreProductsTest() throws InterruptedException {
        selectProductsOnMainPage(2);
        increaseItemQuantity(2); //uruchamianie metod z odpowiednimi wartoscami
        addToCart();
        assertModalHeader();
        continueShopping();
        toGoToHomePage();
        selectProductsOnMainPage(3);
        increaseItemQuantity(1); //uruchamianie metod z odpowiednimi wartoscami
        addToCart();
        assertModalHeader();
        continueShopping();
        toGoToHomePage();
        goToCart();
    }

    private void selectProductsOnMainPage(int index) {
        WebElement productsSection = driver.findElement(By.className("products")); // znalezienie sekcji z produktami
        List<WebElement> productImages = productsSection.findElements(By.tagName("img"));// znalezienie w sekcji z produktami obrazkow
        productImages.get(index).click(); //z elementów productImages, wybierz odpowiedni z indeksem oraz go kliknij
    }

    private void increaseItemQuantity(int amount) {
        WebElement addItem = driver.findElement(By.className("touchspin-up")); //znajdz odpowiedni element o podanej klasie
        for (int i = 0; i < amount; i++) {
            addItem.click();
        }
    }
    private void addToCart() {
        driver.findElement(By.className("add-to-cart")).click();
    }

    private void assertModalHeader() throws InterruptedException{
        Thread.sleep(1000);
        assertTrue(driver.findElement(By.className("modal-header")).getText().contains("Product successfully added to your shopping cart"));
    }

    private void continueShopping(){
        driver.findElement(By.className("btn-secondary")).click();
    }

    private void toGoToHomePage(){
//        driver.findElement(By.xpath("/html/body/main/header/div[2]/div/div[1]/div[1]/a/img")).click();
        driver.get("https://prod-kurs.coderslab.pl/index.php");
    }
private void goToCart(){
        driver.findElement(By.className("shopping-cart")).click();
    }
    private void assertCombinedAmountOfItemsInCheckout(){
   List <WebElement> amountInputs = driver.findElements(By.className("js-cart-line-product-quantity"));
   String firstItemAmount = amountInputs.get(0).getAttribute("value");
   String secondItemAmount = amountInputs.get(1).getAttribute("value");

   int productsSum = Integer.parseInt(firstItemAmount) + Integer.parseInt(secondItemAmount);
   String subTotal = driver.findElement(By.className("js-subtotal")).getText().replace(" items", "");
   assertEquals(productsSum, Integer.parseInt(subTotal));
    }

    @After
    public void tearDown() throws Exception {

        // driver.quit(); // Zamknij przeglądarkę
    }
}