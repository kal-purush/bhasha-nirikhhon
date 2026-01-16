class AddressStepPage {
    private proceedToCheckout: string

    constructor() {
        this.proceedToCheckout = ".cart_navigation > .button > span"
    }

    public goToCheckout(): void{
        cy.get(this.proceedToCheckout).click()
    }
}

export { AddressStepPage } 