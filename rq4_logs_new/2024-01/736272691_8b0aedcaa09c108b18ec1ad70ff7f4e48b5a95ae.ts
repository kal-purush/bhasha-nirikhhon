import { ProductsPage } from "./productsPage"
import { MyCartPage } from "./myCartPage";

export class CartPage {

    productsPage : ProductsPage;
    myCartPage: MyCartPage;

    constructor() {
        this.productsPage = new ProductsPage();
        this.myCartPage = new MyCartPage();
    }

    private selectors = {

        productPriceLabel: '~product price',
        increaseQuantity: '//android.view.ViewGroup[@content-desc="counter plus button"]/android.widget.ImageView',
        decreaseQuantity: '//android.view.ViewGroup[@content-desc="counter minus button"]/android.widget.ImageView',
        // noOfQuantity: '',
        addToCartButton: '//android.widget.TextView[@text="Add To Cart"]',
        productsHighlightsLabel: '//android.widget.TextView[@text="Product Highlights"]',
        cartIcon: '//android.view.ViewGroup[@content-desc="cart badge"]/android.widget.ImageView',

    }

    async getProductPriceLabel() {
        return await $(this.selectors.productPriceLabel);
    }

    async getIncreaseQuantity() {
        return await $(this.selectors.increaseQuantity);
    }

    async getDecreaseQuantity() {
        return await $(this.selectors.decreaseQuantity);
    }

    async getAddToCartButton() {
        return await $(this.selectors.addToCartButton);
    }

    async getProductsHighlightsLabel() {
        return await $(this.selectors.productsHighlightsLabel);
    }

    async getCartIcon() {
        return await $(this.selectors.cartIcon);
    }

    
    async getProductPriceLabelText() {
        const productPriceLabelElement = await $(this.selectors.productPriceLabel);
        await productPriceLabelElement.waitForDisplayed();
        return await productPriceLabelElement.getText();
    }

    async addToCartProduct() {
        
        const fleeceJacketLabelElement = await this.productsPage.getFleeceJacketLabel();
        await fleeceJacketLabelElement.waitForDisplayed();

        for (let i = 0; i < 3; i++) {
            (await this.getIncreaseQuantity()).click();
        }

        (await this.getAddToCartButton()).click();
        (await this.getCartIcon()).click();

        const removeProduct = await this.myCartPage.getRemoveItem();
        await removeProduct.waitForDisplayed();
        await removeProduct.click();
    }



}