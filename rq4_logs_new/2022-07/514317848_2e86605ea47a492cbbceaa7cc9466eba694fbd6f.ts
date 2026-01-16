export class ShippersPage{
    visit(){
     cy.visit('/shippers')
    }
    getNewShippersBtn(){
        return cy.get('app-create-user button.custom-btn-green')
    }
}