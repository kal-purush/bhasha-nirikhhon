export class RecipientsPage{
    getRecipiensList(){
        return cy.get('.sidebar ul:nth-child(1) a[title="Recipients"]')
    }
    getNewRecipientsBtn(){
        return cy.get('app-recipients .custom-btn-green')
    }
    getFirstStep(){
        return cy.get('app-stepper-header li:nth-child(1)')
    }
    getCompany(value){
        const field = cy.get('.form-group input[formcontrolname="company"]')
        field.clear()
        field.type(value)
        return this
    }
    getInvalodFeadbackCompany(){
        return cy.contains('Company Name required, at least 3 and no more than 100 characters')
    }
    focusFirstName(){
        const field = cy.get('.form-group input[formcontrolname="firstName"]')
        field.focus()
    }
    getFirstName(value){
        const field = cy.get('.form-group input[formcontrolname="firstName"]')
        field.clear()
        field.type(value)
        return this
    }
    getInvalodFeadbackFirstName(){
        return cy.contains('First Name required, at least 1 and no more than 50 characters')
    }
    focusLastName(){
        const field = cy.get('.form-group input[formcontrolname="lastName"]')
        field.focus()
    }
    getLastName(value){
        const field = cy.get('.form-group input[formcontrolname="lastName"]')
        field.clear()
        field.type(value)
        return this
    }
    getInvalodFeadbackLastName(){
        return cy.contains('Last Name required, at least 1 and no more than 50 characters')
    }
    focusPhoneNumber(){
        const field = cy.get('.form-group input[formcontrolname="phone"]')
        field.focus()
    }
    getPhoneNumber(value){
        const field = cy.get('.form-group input[formcontrolname="phone"]')
        field.clear()
        field.type(value)
        return this
    }
    getInvalodFeadbackPhone(){
        return cy.contains('Phone Number required, at least 7 and no more than 18 numbers, including international country code')
    }
    focusEmailAdress(){
        const field = cy.get('.form-group input[formcontrolname="email"]')
        field.focus()
    }
    getEmailAdress(value){
        const field = cy.get('.form-group input[formcontrolname="email"]')
        field.clear()
        field.type(value)
        return this
    }
    getInvalodFeadbackEmail(){
        return cy.contains('Email Address required')
    }
    getNextBtn(){
        return cy.get('app-stepper-footer button')
    }
    getLastStep(){
        return cy.get('app-stepper-header li:nth-child(1)')
    }
    focusAdressLine1(){
        const field = cy.get('.form-group input[formcontrolname="address1"]')
        field.focus()
    }
    getAdressLine1(value){
        const field = cy.get('.form-group input[formcontrolname="address1"]')
        field.clear()
        field.type(value)
        return this

    }
    getInvalodFeadbackAdress1(){
        return cy.contains('Address Line 1 required, at least 1 and no more than 100 characters')
    }
    getAdressLine2(value){
        const field = cy.get('.form-group input[formcontrolname="address2"]')
        field.clear()
        field.type(value)
        return this
        
    }
    getState(){

        return cy.get('select[formcontrolname="state"]')
    }
    focusCity(){
        const field = cy.get('.form-group input[formcontrolname="city"]')
        field.focus()
    }
    getCity(value){
        const field = cy.get('.form-group input[formcontrolname="city"]')
        field.clear()
        field.type(value)
        return this

    }
    getInvalodFeadbackCity(){
        return cy.contains('City required, at least 1 and no more than 100 characters')
        
    }
    focusZipCode(){
        const field = cy.get('.form-group input[formcontrolname="zipCode"]')
        field.focus()
    }
    getZipCode(value){

        const field = cy.get('.form-group input[formcontrolname="zipCode"]')
        field.clear()
        field.type(value)
        return this
    }
    getInvalodFeadbackZipCode(){
        return cy.contains('Zip Code required, at least 5 numbers and no letters') 
    }
    getStepNextBtn(){
        return cy.get('app-stepper-footer button.prev-step')
    }
    getThirthStep(){
            return cy.get('app-stepper-header li:nth-child(3)')
    }
    getRecipientListCompany(){
        return cy.get('.recipient-list li:nth-child(1) b')
    }
    getRecipientListFirstName(){
        return cy.get('.recipient-list li:nth-child(2) b')
    }
    getRecipientListLastName(){
        return cy.get('.recipient-list li:nth-child(3) b')
    }
    getRecipientListPhone(){
        return cy.get('.recipient-list li:nth-child(4) b') 
    }
    getRecipientListEmail(){
        return cy.get('.recipient-list li:nth-child(5) b') 
    }
    getRecipientListAdress1(){
        return cy.get('.recipient-list li:nth-child(6) b') 
    }
    getRecipientListAdress2(){
        return cy.get('.recipient-list li:nth-child(7) b') 
    }
    
    getRecipientListState(){
        return cy.get('.recipient-list li:nth-child(8) b') 
    }
    getRecipientListCity(){
        return cy.get('.recipient-list li:nth-child(9) b') 
    }
    getRecipientListZipCode(){
        return cy.get('.recipient-list li:nth-child(10) b') 
    }
    getSubmitBtn(){
        return cy.get('app-stepper-footer button.prev-step')
    }
}