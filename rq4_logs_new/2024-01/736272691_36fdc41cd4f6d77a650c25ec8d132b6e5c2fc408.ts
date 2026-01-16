import { BaseActions } from "../utilities/baseActions";
import { ProductsScreen } from "../screens/productScreen";


let productsScreen: ProductsScreen;
let baseActions: BaseActions;

describe("Swipe Gestures", () => {

    before(async () => {
        productsScreen = new ProductsScreen();
        baseActions = new BaseActions();
    });


    it('Should scroll until an element is visible on a mobile app', async () => {
        const footerEle = await productsScreen.getFooterLabel();
        await baseActions.swipe(footerEle);
    })

})