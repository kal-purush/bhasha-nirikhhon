package pages.CartSummary;

import configuration.PageBase;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.testng.Assert;
import utils.CRUD.Cart;
import utils.CRUD.Product;

import java.util.List;

public class CartSummary extends PageBase {
    private CartSummaryElements webElements;

    private CartSummary(WebDriver driver){
        super(driver);
    }

    public static CartSummary of(WebDriver driver){
        return new CartSummary(driver);
    }

    //region Web Elements
    private By ProductImage() { return By.cssSelector(webElements.PRODUCT_IMAGE); }
    private By NameLabel() { return By.cssSelector(webElements.NAME_LABEL); }
    private By SkuLabel() { return By.cssSelector(webElements.SKU_LABEL); }
    private By ColorAndSizeLabel() { return By.cssSelector(webElements.COLOR_AND_SIZE_LABEL); }
    private By QuantityInput() { return By.cssSelector(webElements.QUANTITY_INPUT); }
    private By ItemTotalPriceLabel() { return By.cssSelector(webElements.ITEM_TOTAL_PRICE_LABEL); }
    private By TotalPriceProductsLabel() { return By.cssSelector(webElements.TOTAL_PRICE_PRODUCTS_LABEL); }
    private By TotalShippingLabel() { return By.cssSelector(webElements.TOTAL_SHIPPING_LABEL); }
    private By TotalPriceWithoutTaxLabel() { return By.cssSelector(webElements.TOTAL_PRICE_WITHOUT_TAX_LABEL); }
    private By TotalTaxLabel() { return By.cssSelector(webElements.TOTAL_TAX_LABEL); }
    private By TotalFinalPrice() { return By.cssSelector(webElements.TOTAL_FINAL_PRICE); }
    private By ProceedToCheckoutButton() { return By.cssSelector(webElements.PROCEED_TO_CHECKOUT_BUTTON); }
    //endregion

    //region Actions
    public CartSummary validateProductInformation(Cart cart){
        waitForElement(ProductImage());
        for(int i = 0; i < cart.getProducts().size(); i++){
            String[] colorSize = driver.findElements(ColorAndSizeLabel()).get(i).getText().split(", ");
            Assert.assertTrue(cartContainsAddedItems(cart.getProducts(),
                    new Product(
                            driver.findElements(NameLabel()).get(i).getText(),
                            driver.findElements(SkuLabel()).get(i).getText().split(": ")[1],
                            Integer.parseInt(driver.findElements(QuantityInput()).get(i).getAttribute("value")),
                            colorSize[1].split(": ")[1].charAt(0),
                            colorSize[0].split(": ")[1],
                            driver.findElements(ProductImage()).get(i).getAttribute("src"),
                            Float.parseFloat(driver.findElements(ItemTotalPriceLabel()).get(i).getText()
                                    .replace("$",""))
                    )
            ));
        }
        return this;
    }
    public CartSummary validateCartInformation(Cart cart){
        float totalWithoutTax = cart.getShipping() + cart.getCartTotal();
        float tax = Float.parseFloat(driver.findElement(TotalTaxLabel()).getText().replace("$",""));

        Assert.assertEquals(driver.findElement(TotalPriceProductsLabel()).getText().replace("$",""),
                String.format("%.02f", cart.getCartTotal()));
        Assert.assertEquals(driver.findElement(TotalShippingLabel()).getText().replace("$",""),
                String.format("%.02f", cart.getShipping()));
        Assert.assertEquals(driver.findElement(TotalPriceWithoutTaxLabel()).getText().replace("$",""),
                String.format("%.02f", totalWithoutTax));
        Assert.assertEquals(driver.findElement(TotalFinalPrice()).getText().replace("$",""),
                String.format("%.02f", totalWithoutTax + tax));

        return this;
    }
    public CartSummary clickProceedToCheckoutButton(){
        click(ProceedToCheckoutButton());
        return this;
    }
    //endregion

    //region Helpers
    private boolean cartContainsAddedItems(List<Product> addedProducts, Product cartProduct){
        return addedProducts.stream().anyMatch(o ->
                o.getImageURL().replace("large", "small").equals(cartProduct.getImageURL()) &&
                o.getName().toUpperCase().equals(cartProduct.getName().toUpperCase()) &&
                o.getSku().equals(cartProduct.getSku()) && o.getColor().equals(cartProduct.getColor()) &&
                o.getSize() == cartProduct.getSize() && o.getQuantity() == cartProduct.getQuantity() &&
                o.getTotalPrice() == cartProduct.getTotalPrice());
    }
    //endregion
}