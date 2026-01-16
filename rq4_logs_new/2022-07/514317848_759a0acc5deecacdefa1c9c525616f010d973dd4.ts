//.card button.btn-primary
export class RecipientDashboardPage {
    getEditViewBtn()
    {
        return cy.get('.card button.btn-primary')
    }
    getCompanyName(){
        return cy.get('.ng-valid input:nth-of-type(1)')
    }
    getContactFirstName(){
        return cy.get('.ng-valid input:nth-of-type(1)[formcontrolname="firstName"]')
    }
    getContactLastName(){
        return cy.get('.ng-valid input:nth-of-type(1)[formcontrolname="lastName"]')
    }
    getPhoneNumber(){
        return cy.get('.ng-valid input:nth-of-type(1)[formcontrolname="phone"]')
    }
    getEmailAdress(){
        return cy.get('.ng-valid input:nth-of-type(1)[formcontrolname="email"]')
    }
    getNextBtn(){
        return cy.get('.col-sm-12 .list-inline  button.prev-step')
    }
    getAddressLine1(){
        return cy.get('.ng-valid input:nth-of-type(1)[formcontrolname="address1"]')
    }
    getAddressLine2(){
        return cy.get('.ng-valid input:nth-of-type(1)[formcontrolname="address2"]')
    }
    getState(){
        return cy.get('.ng-valid select[formcontrolname="state"]')
    }
    getCity(){
        return cy.get('.ng-valid input:nth-of-type(1)[formcontrolname="city"]')
    }
    getZipCode(){
        return cy.get('.ng-valid input:nth-of-type(1)[formcontrolname="zipCode"]')
    }
    getRecipientListCompany(){
        return cy.get('app-edit-user-form .recipient-list li:nth-child(1) b')
    }
    getRecipientListFirstName(){
        return cy.get('app-edit-user-form .recipient-list li:nth-child(2) b')
    }
    getRecipientListLastName(){
        return cy.get('app-edit-user-form .recipient-list li:nth-child(3) b')
    }
    getRecipientListPhone(){
        return cy.get('app-edit-user-form .recipient-list li:nth-child(4) b') 
    }
    getRecipientListEmail(){
        return cy.get('app-edit-user-form .recipient-list li:nth-child(5) b') 
    }
    getRecipientListAddress1(){
        return cy.get('app-edit-user-form .recipient-list li:nth-child(6) b') 
    }
    getRecipientListAddress2(){
        return cy.get('app-edit-user-form .recipient-list li:nth-child(7) b') 
    }
    
    getRecipientListState(){
        return cy.get('app-edit-user-form .recipient-list li:nth-child(8) b') 
    }
    getRecipientListCity(){
        return cy.get('app-edit-user-form .recipient-list li:nth-child(9) b') 
    }
    getRecipientListZipCode(){
        return cy.get('app-edit-user-form .recipient-list li:nth-child(10) b')
    }
    getSubmitBtn(){
        return cy.get('app-edit-user-form .list-inline button')
    }
}