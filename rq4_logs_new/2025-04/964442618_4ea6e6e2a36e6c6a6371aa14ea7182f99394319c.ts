import {
    Homepage,
    GaragePage,
    AddCarPopUp,
    CarItem,
    LoginPopUp,
    EditCarPopUp,
    RemoveCarPopUp,
} from "../poms";
import { test, expect } from "@playwright/test";

let homepage: Homepage;
let loginPopUp: LoginPopUp;
let garagePage: GaragePage;
let addCarPopUp: AddCarPopUp;
let carItem: CarItem;
let editCarPopUp: EditCarPopUp;
let removeCarPopUp: RemoveCarPopUp;

test.describe("Testing garage flow", () => {
    test.beforeEach(async ({ page }) => {
        homepage = new Homepage(page);
        await page.goto("/");
        await homepage.signInButton.click();

        loginPopUp = new LoginPopUp(page);
        await loginPopUp.verifyIsVisible(true);
        await loginPopUp.verifyPopUpTitle("Log in");
        await loginPopUp.fillAndSubmitForm(
            process.env.userEmail, process.env.userPassword
        );

        await loginPopUp.verifyIsVisible(false);
        await expect(page).toHaveURL("/panel/garage");
    });

    test("Test user can create, edit and delete auto", async ({ page }) => {
        await test.step("Add a car", async () => {
            garagePage = new GaragePage(page);
            await garagePage.clickAddCarButton();

            addCarPopUp = new AddCarPopUp(page);
            await addCarPopUp.verifyIsVisible(true);
            await addCarPopUp.verifyPopUpTitle("Add a car");
            await addCarPopUp.selectCar("Porsche", "Cayenne", "340123");
            await addCarPopUp.addButton.click();
            await addCarPopUp.verifyIsVisible(false);
        });

        await test.step("Verify that car was added to the table", async () => {
            carItem = new CarItem(page);
            await expect(carItem.item).toBeVisible();
            await expect(carItem.carName).toHaveText("Porsche Cayenne");
            await expect(carItem.updateMileageInput).toHaveValue("340123");
        });

        await test.step("Make changes to created car", async () => {
            await carItem.editButton.click();

            editCarPopUp = new EditCarPopUp(page);
            await editCarPopUp.verifyIsVisible(true);
            await editCarPopUp.verifyPopUpTitle("Edit a car");
            await editCarPopUp.selectCar("Audi", "TT", "340124");
            await editCarPopUp.saveButton.click();
            await editCarPopUp.verifyIsVisible(false);
        });

        await test.step("Verify changes have been applied", async () => {
            await expect(carItem.item).toBeVisible();
            await expect(carItem.carName).toHaveText("Audi TT");
            await expect(carItem.updateMileageInput).toHaveValue("340124");
        });

        await test.step("Remove car", async () => {
            await carItem.editButton.click();

            editCarPopUp = new EditCarPopUp(page);
            await editCarPopUp.verifyIsVisible(true);
            await editCarPopUp.verifyPopUpTitle("Edit a car");
            await editCarPopUp.clickRemoveCarButton();
            await editCarPopUp.verifyIsVisible(false);
            
            removeCarPopUp = new RemoveCarPopUp(page);
            await removeCarPopUp.verifyIsVisible(true);
            await removeCarPopUp.verifyPopUpTitle("Remove car");
            await removeCarPopUp.clickRemoveButton();
            await removeCarPopUp.verifyIsVisible(false);
        });

        await test.step("Check car is not present in the list", async () => {
            await expect(carItem.item).not.toBeVisible();
        });
    });
});