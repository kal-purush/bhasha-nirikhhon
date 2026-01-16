import { CartScreen } from "../screens/cartScreen";
import { MyCartScreen } from "../screens/myCartScreen";
import { HomeScreen } from "../screens/homeScreen";

export class CartUtil {

    cartScreen: CartScreen;
    myCartScreen: MyCartScreen;
    homeScreen: HomeScreen;

    constructor() {
        this.cartScreen = new CartScreen();
        this.myCartScreen = new MyCartScreen();
        this.homeScreen = new HomeScreen();
    }

    async addToCart(productName: string, addedQuantity:number) {
        const productElement = await this.homeScreen.findProductElementByName(productName);

        if (productElement) {
            await productElement.click();

            const addToCartButton = await this.cartScreen.getAddToCartButton();

            if(addToCartButton) {
                await addToCartButton.waitForDisplayed();
                await this.cartScreen.increaseQuantity(addedQuantity);
                await addToCartButton.click();
            } else {
                throw new Error(`Add to Cart button not found for product: ${productName}`);
            }    
        } else {
            throw new Error(`Product not found: ${productName}`);
        }
    }
}