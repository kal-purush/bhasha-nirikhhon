import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import data.Product;
import data.ProductDataProvider;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class AutoTests extends TestBase{

    @Test
    public void homeWork1Test() {
        app.login();
        app.checkMenuItems();
        app.logout();
    }

    @Test
    public void homeWork2Test() {
        app.openMainApp();
        app.checkProductOpen();
    }

    @Test
    @UseDataProvider(value = "validProducts", location = ProductDataProvider.class)
    public void homeWork3Part1Test(Product product) {
        app.login();
        app.createNewProduct(product);
        app.logout();
    }

    @Test
    public void homeWork3Part2Test() {
        app.openMainApp();
        app.productAddToCart();
        app.productAddToCart();
        app.productAddToCart();
        app.productRemoveFromCart();
    }


    @Test
    public void homeWork4Part1Test() {
        app.login();
        app.checkCountryLinks();
        app.logout();
    }
}