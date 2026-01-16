package pages.ProductDetails;

import configuration.PageBase;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import utils.CRUD.Product;
import utils.CommonMethods;

import java.util.List;

public class ProductDetails extends PageBase {
    private ProductDetailsElements webElements;

    private ProductDetails(WebDriver driver){
        super(driver);
    }

    public static ProductDetails of(WebDriver driver){
        return new ProductDetails(driver);
    }

    //region Web Elements
    private By NameLabel() { return By.cssSelector(webElements.NAME_LABEL); }
    private By SkuLabel() { return By.cssSelector(webElements.SKU_LABEL); }
    private By CurrentPriceLabel() { return By.cssSelector(webElements.CURRENT_PRICE_LABEL); }
    private By ReductionLabel() { return By.cssSelector(webElements.REDUCTION_LABEL); }
    private By OldPriceLabel() { return By.cssSelector(webElements.OLD_PRICE_LABEL); }
    private By QuantityInput() { return By.cssSelector(webElements.QUANTITY_INPUT); }
    private By SizeCombo() { return By.cssSelector(webElements.SIZE_COMBO); }
    private By ColorList() { return By.cssSelector(webElements.COLOR_LIST); }
    private By ColorOptions() { return By.cssSelector(webElements.COLOR_OPTIONS); }
    private By AddToCartButton() { return By.cssSelector(webElements.ADD_TO_CART_BUTTON); }
    //endregion

    //region Actions
    public ProductDetails enterQuantity(){
        fillString(QuantityInput(), String.valueOf(CommonMethods.randomNumber(1, 5)));
        return this;
    }
    public ProductDetails chooseSize(){
        click(SizeCombo());
        List<WebElement> options = driver.findElement(SizeCombo()).findElements(By.tagName("option"));
        click(options.get(CommonMethods.randomNumber(0, options.size() - 1)));
        return this;
    }
    public ProductDetails chooseColor(){
        waitForElement(ColorList());
        List<WebElement> colors = driver.findElement(ColorList()).findElements(ColorOptions());
        click(colors.get(CommonMethods.randomNumber(0, colors.size() - 1)));
        return this;
    }
    public Product saveProductInfo(){
        int reduction = 0;
        float oldPrice = 0;

        if(isPresent(ReductionLabel())){
            reduction = Integer.parseInt(driver.findElement(ReductionLabel()).getText().substring(1)
                    .replace("%", ""));
            oldPrice = Float.parseFloat(driver.findElement(OldPriceLabel()).getText().replace("$", ""));
        }

        return new Product(
                driver.findElement(NameLabel()).getText(),
                driver.findElement(SkuLabel()).getText(),
                Float.parseFloat(driver.findElement(CurrentPriceLabel()).getText().replace("$", "")),
                reduction,
                oldPrice,
                Integer.parseInt(driver.findElement(QuantityInput()).getText()),
                driver.findElement(SizeCombo()).findElement(By.tagName("selected")).getText().charAt(0),
                driver.findElement(ColorList()).findElement(By.className("selected")).getAttribute("name")
        );
    }
    //endregion
}