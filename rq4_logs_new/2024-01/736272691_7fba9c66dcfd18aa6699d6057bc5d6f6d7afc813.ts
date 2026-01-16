import { BaseActions } from "../../commonFunctions/baseActions";
import { ProductsScreen } from "../../screens/productScreen";

let productsScreen: ProductsScreen;
let baseActions: BaseActions;

describe('***** Press and Hold Gestures *****', () => {

    before(async () => {
        productsScreen = new ProductsScreen();
        baseActions = new BaseActions();
    });

    afterEach(async () => {
        
        // Terminate and Launch the driver again
        await driver.terminateApp("com.saucelabs.mydemoapp.rn");
        await driver.activateApp("com.saucelabs.mydemoapp.rn");
    });

    it('Performs press and hold gesture on a mobile element', async () => {
        await productsScreen.pressHoldFirstItem();
    });

    it('Performs press and hold gesture on a mobile element using coordinates', async () => {
        await productsScreen.PressHoldOffsetFirstItem();
    });
});