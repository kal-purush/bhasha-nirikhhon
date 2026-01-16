import { $ } from "@wdio/globals";
import { ShippingAddressUi } from "../resources/shippingAddressUi.ts";
import { CardDetails } from "../resources/cardDetailsUi.ts"

export class CheckOutScreen {

    private selectors = {

        checkoutHeader: "//android.view.ViewGroup[@content-desc='container header']/android.widget.TextView[@text='Checkout']",
        fullNameInput: "~Full Name* input field",
        addressLineOneInput: "~Address Line 1* input field",
        addressLineTwoInput: "~Address Line 2 input field",
        cityInput: "~City* input field",
        stateInput: "~State/Region input field",
        zipCodeInput: "~Zip Code* input field",
        countryInput: "~Country* input field",
        toPaymentButton: "~To Payment button",
        cardNumberInput: "~Card Number* input field",
        expirationDateInput: "~Expiration Date* input field",
        securityCodeInput: "~Security Code* input field",
        reviewOrderButton: "~Review Order button",
        placeOrderButton: "~Place Order button",
        continueShoppingButton: "~Continue Shopping button",

    }

    async enterFullName(fullName: string) {
        const fullNameInputFieldEle = await $(this.selectors.fullNameInput);
        await fullNameInputFieldEle.setValue(fullName);
    }

    async enterAddressLine1(addressLine1: string) {
        const addressLine1InputFieldEle = await $(this.selectors.addressLineOneInput);
        await addressLine1InputFieldEle.setValue(addressLine1);
    }

    async enterAddressLine2(addressLine2: string) {
        const addressLine2InputFieldEle = await $(this.selectors.addressLineTwoInput);
        await addressLine2InputFieldEle.setValue(addressLine2);
    }

    async enterCity(cityName: string) {
        const cityInputFieldEle = await $(this.selectors.cityInput);
        await cityInputFieldEle.setValue(cityName);
    }

    async enterState(stateName: string) {
        const stateInputFieldEle = await $(this.selectors.stateInput);
        await stateInputFieldEle.setValue(stateName);
    }

    async enterZipCode(zipCode: number) {
        const zipCodeInputFieldEle = await $(this.selectors.zipCodeInput);
        await zipCodeInputFieldEle.setValue(zipCode.toString());
    }

    async enterCountry(countryName: string) {
        const countryInputFieldEle = await $(this.selectors.countryInput);
        await countryInputFieldEle.setValue(countryName);
    }

    async clickToPaymentButton() {
        const toPaymentButtonEle = await $(this.selectors.toPaymentButton);
        await toPaymentButtonEle.click();
    }

    async enterCardNumber(cardNumber: string) {
        const cardNumberInputFieldEle = await $(this.selectors.cardNumberInput);
        await cardNumberInputFieldEle.setValue(cardNumber);
    }

    async enterExpirationDate(expirationDate: string) {
        const expirationDateInputFieldEle = await $(this.selectors.expirationDateInput);
        await expirationDateInputFieldEle.setValue(expirationDate);
    }

    async enterSecurityCode(securityCode: number) {
        const securityCodeInputFieldEle = await $(this.selectors.securityCodeInput);
        await securityCodeInputFieldEle.setValue(securityCode.toString());
    }

    async clickReviewOrderButton() {
        const reviewOrderButtonEle = await $(this.selectors.reviewOrderButton);
        await reviewOrderButtonEle.click();
    }

    async clickPlaceOrderButton() {
        const placeOrderButtonEle = await $(this.selectors.placeOrderButton);
        await placeOrderButtonEle.waitForDisplayed();
        await placeOrderButtonEle.click();
    }

    async clickContinueShoppingButton() {
        const continueShoppingButtonEle = await $(this.selectors.continueShoppingButton);
        await continueShoppingButtonEle.click()
    }

    async enterShippingAddressDetails(shippingAddressDetails: ShippingAddressUi) {
        await this.enterFullName(shippingAddressDetails.fullName);
        await this.enterAddressLine1(shippingAddressDetails.addressLine1);
        await this.enterAddressLine2(shippingAddressDetails.addressLine2);
        await this.enterCity(shippingAddressDetails.city);
        await this.enterState(shippingAddressDetails.state);
        await this.enterZipCode(shippingAddressDetails.zipCode);
        await this.enterCountry(shippingAddressDetails.country);
        await driver.hideKeyboard();
    }

    async enterCardDetails(cardDetails: CardDetails) {
        await this.enterFullName(cardDetails.fullName);
        await this.enterCardNumber(cardDetails.cardNumber);
        await this.enterExpirationDate(cardDetails.expirationDate);
        await this.enterSecurityCode(cardDetails.securityCode);
        await driver.hideKeyboard();
    }

}