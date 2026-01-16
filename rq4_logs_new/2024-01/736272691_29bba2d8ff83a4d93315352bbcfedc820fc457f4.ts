import { LOGGER, LoggerHelper } from "../../../customLogger/loggerHelper.ts";
import { LoginScreen } from "../../screens/loginScreen.ts";
import { HomeScreen } from "../../screens/homeScreen.ts";
import { CartScreen } from "../../screens/cartScreen.ts";
import { MyCartScreen } from "../../screens/myCartScreen.ts";
import { LoginDetails } from "../../resources/customTypes/loginDetails.ts";
import * as loginDetailsJson from "../../resources/testdata/loginDetails.json"
import { FileUtils } from "../../../utilities/fileUtil.ts";
import { ProductDetails } from "../../resources/customTypes/productDetails.ts";
import { CartUtilScreen } from "../../commonFunctions/cartUtilScreen.ts";
import * as productDetailJson from "../../resources/testdata/productDetails.json"
import { CheckOutScreen } from "../../screens/checkOutScreen.ts";
import { ShippingAddress } from "../../resources/customTypes/shippingAddress.ts";
import { CardDetails } from "../../resources/customTypes/cardDetails.ts"
import * as shippingAddressDetailsJson from "../../resources/testdata/shippingAddressDetails.json"
import * as cardDetailsJson from "../../resources/testdata/cardDetails.json"

let loginScreen: LoginScreen;
let homeScreen: HomeScreen;
let cartScreen: CartScreen;
let myCartScreen: MyCartScreen;
let loginDetails: LoginDetails;
let productDetails: ProductDetails[];
let cartUtilScreen: CartUtilScreen;
let checkOutScreen : CheckOutScreen;

const specName = 'Add multiple products';

describe("Add multiple products", () => {

    before(async () => {
        LoggerHelper.setupLogger(specName);
        loginScreen = new LoginScreen();
        homeScreen = new HomeScreen();
        cartScreen = new CartScreen();
        myCartScreen = new MyCartScreen();
        cartUtilScreen = new CartUtilScreen();
        checkOutScreen = new CheckOutScreen();
        loginDetails = FileUtils.convertJsonToCustomType(loginDetailsJson);
        await loginScreen.login(loginDetails.username, loginDetails.password);
        productDetails = FileUtils.convertJsonToCustomType(productDetailJson);
    });

    it('Add multiple products to the cart', async function () {

        const shippingAddressDetails: ShippingAddress = FileUtils.convertJsonToCustomType(shippingAddressDetailsJson);
        const cardDetails: CardDetails = FileUtils.convertJsonToCustomType(cardDetailsJson);

        try {
            const product1Quantity = 3;
            await cartUtilScreen.addToCart(productDetails[0].name, product1Quantity);
            await driver.back();

            const product2Quantity = 2;
            await cartUtilScreen.addToCart(productDetails[1].name, product2Quantity);
            await driver.back();

            await cartScreen.getCartIcon();
            const cartItems = await cartUtilScreen.getCartItems();
            LOGGER.info(`Number of cart items found: ${cartItems.length}`);

            LOGGER.info(`Actual Cart Items:", ${cartItems}`);
            expect(cartItems.length).toBe(2);

            expect(cartItems).toContain(productDetails[0].name);
            expect(cartItems).toContain(productDetails[1].name);

            await myCartScreen.clickProceedToCheckoutButton();
            await checkOutScreen.enterShippingAddressDetails(shippingAddressDetails);
            await checkOutScreen.clickToPaymentButton();
            await checkOutScreen.enterCardDetails(cardDetails);
            await checkOutScreen.clickReviewOrderButton();
            await checkOutScreen.clickPlaceOrderButton();

            const orderConfirmationEle = await checkOutScreen.getOrderConfirmationEle();
            const orderConfirmationText = await orderConfirmationEle.getText();
            expect(orderConfirmationText).toBe(' Your order has been dispatched and will arrive as fast as the pony gallops!');
            await checkOutScreen.clickContinueShoppingButton();

        } catch (error) {
            LOGGER.error(`Error during test execution: ${(error as Error).message}`);
            throw error;
        }
    });
})