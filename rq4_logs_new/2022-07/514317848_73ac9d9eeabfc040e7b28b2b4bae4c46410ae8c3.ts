export class ProductsPage{
    getProductsList(){
        return cy.get('.sidebar-nav a[title="Products"]')
    }
    getNewProductsBtn(){
        return cy.get('app-products button.custom-btn-green')
    }
    getSingleTypeCheckbox(){
        return cy.get('.modal.show .modal-body > app-stepper input[value="single"]')
    }
    getFirstStep(){
        return cy.get('app-stepper-header li:nth-child(1)')
    }
    getNextBtn(){
        return cy.get('.modal.show .modal-body > app-stepper > app-stepper-footer button')
    }
    getFocusName(){
        return cy.get('#name')
    }
    getName(){
        return cy.get('#name')
    }
    getInvalidFedbackName(){
        return cy.contains('Product name is required')
    }
    getFocusDescription(){
        return cy.get('#description')
    }
    getDescription(){
        return cy.get('#description')
    }
    getInvalidFedbackDescription(){
        return cy.contains('Product description is required')
    }
    getOptionalId(){
        return cy.get('#optional_product_id')
    }
    getOriginCountry(){
        //app-body-stepper select.form-control
        return cy.get('.modal-body app-stepper-step:nth-child(3) form:nth-child(1) div.form-group:nth-child(5) select')
    }
    getFocusFDAcode(){

        return cy.get('#fda_product_code')
    }
    getFDAcode(){

        return cy.get('#fda_product_code')
    }
    getInvalidFedbackFDAcode(){
        return cy.contains('FDA Product Code is invalid')
    }
    getLastStep(){
        return cy.get('.modal.show app-stepper-header li:nth-child(1)')
    }
    getRadioMerchantYes(){
        return cy.get('#radioMerchantYes')
    }
    getRadioShipperYes(){
        return cy.get('#radioShipperYes')
    }
    getRadioMerchantNo(){
        return cy.get('#radioMerchantNo')
    }
    getRadioShipperNo(){
        return cy.get('#radioShipperNo')
    }
    getThirthStep(){
        return cy.get('.modal.show app-stepper-header li:nth-child(3)')
    }
    getCheckboxProduct(){
        return cy.get('input.checkbox-sold')
    }
    getInputProductPackaging(){
        return cy.get('.edit-left input#total_weight')
    }
    getPoundsRadioBtn(){
        return cy.get('.radio-btn input[value="Pounds"')
    }
    getOuncesRadioBtn(){
        return cy.get('.radio-btn input[value="Ounces"]')
    }
    getGramsRadioBtn(){
        return cy.get('.radio-btn input[value="Grams"]')
    }
    getKilogramsRadioBtn(){
        return cy.get('.radio-btn input[value="Kilograms"]')
    }
    getLitersRadioBtn(){
        return cy.get('.radio-btn input[value="Liters"]')
        
    }
    getSubmitBtn(){
        return cy.get('.modal.show .modal-body > app-stepper > app-stepper-footer  button.prev-step')
    }


}