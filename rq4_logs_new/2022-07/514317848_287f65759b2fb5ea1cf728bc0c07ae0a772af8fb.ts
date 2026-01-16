import {StatusPage} from './page-object/StatusPage'
import {MerchantsPage} from './page-object/MerchantsPage'
import {RecipientsPage} from './page-object/RecipientsPage'
describe('Create  new merchants', () => {
    beforeEach(()=>{
        cy.clearLocalStorage('loggedInUserData')
        cy.clearLocalStorage('token')
        cy.viewport(1920, 1080)
       cy.intercept('POST',/\/users\/change-card/,{fixture: 'interceptChangeCardFixtures.json'}).as('PostCard-Fixtures')
       cy.intercept('POST',/\/users/,{fixture:'interceptUsersFixtures.json'}).as('GetUsers-Fixtures')
       cy.intercept('GET','/users?page=1&perPage=10&type=1',{fixture:'interceptUsersTypeFixtures.json'}).as('GetUsersType-Fixtures')
         
    })
    it('Should check validation form add merchants', () => {
        cy.login()
      const statusPage = new StatusPage()
      statusPage
        .visit()
      const merchantsPage = new MerchantsPage()
      merchantsPage
        .visit()
      merchantsPage
        .getNewMerchantsBtn()
        .click()
      merchantsPage
        .getCountry()
        .select(1)
      const recipientsPage = new RecipientsPage()
      recipientsPage
      recipientsPage
      .getFirstStep().should('have.class','active')
      merchantsPage
        .focusCompany()
        .focus()
      recipientsPage
        .focusFirstName()
        .focus()
      recipientsPage
        .getInvalidFeadbackFirstName()
        .should('have.text','First Name required, at least 1 and no more than 50 characters')
      recipientsPage
        .focusLastName()
        .focus()
      recipientsPage
        .getInvalidFeadbackLastName()
        .should('have.text','Last Name required, at least 1 and no more than 50 characters')
      recipientsPage
        .focusPhoneNumber()
        .focus()
      recipientsPage
        .getInvalidFeadbackPhone()
        .should('have.text','Phone Number required, at least 7 and no more than 18 numbers, including international country code')
      recipientsPage
        .focusEmailAddress()
        .focus()
      recipientsPage
        .getInvalidFeadbackEmail()
        .should('have.text','Email Address required')
      merchantsPage
        .getCountry().select(2)
      recipientsPage
        .getCompany()
        .type('Drive')
      recipientsPage
        .getFirstName()
        .type('Antonenko')
      recipientsPage
        .getLastName()
        .type('Vladislava')
      recipientsPage
        .getPhoneNumber()
        .type('423423')
      recipientsPage
        .getInvalidFeadbackPhone()
        .should('have.text','Phone Number required, at least 7 and no more than 18 numbers, including international country code')
      recipientsPage
        .getEmailAddress()
        .type('sdad@gmailcom')
      recipientsPage
        .getInvalidFeadbackEmail()
        .should('have.text','Email Address required')
      recipientsPage
        .getNextBtn()
        .should('be.disabled')
      recipientsPage
        .getPhoneNumber()
        .clear()
        .type('+38097423423')
      recipientsPage
        .getEmailAddress()
        .clear()
        .type('driveofficall@gmail.com')
        recipientsPage
        .getNextBtn()
        .should('be.enabled')
        .click()
        recipientsPage
        .getLastStep().should('have.class','completed')
    //two pge recipient
    recipientsPage
        .focusAdressLine1()
    recipientsPage
        .focusCity()
    recipientsPage
        .getInvalidFeadbackAdress1()
        .should('have.text','Address Line 1 required, at least 1 and no more than 100 characters')
    recipientsPage
        .focusZipCode()
        .focus()
    recipientsPage
        .getInvalidFeadbackCity()
        .should('have.text','City required, at least 1 and no more than 100 characters')
    recipientsPage
        .getAddressLine1()
        .clear()
        .type('dfdfsfsfsf')
    merchantsPage
        .getInvalidFeadbackZipCode()
        .should('have.text','ZIP/Postal Code required, at least 1 and no more than 15 characters')
    merchantsPage
        .getNextBtn().should('be.disabled')
    recipientsPage
        .getAddressLine1()
        .clear()
        .type('42 Bald Hill Street Dallas TX 75228')
    recipientsPage
        .getAddressLine2()
        .clear()
        .type('1 George Lane Houston TX 77096')
    recipientsPage
        .getCity()
        .clear()
        .type('Texas')
    recipientsPage
        .getZipCode()
        .clear()
        .type('123')
   
    recipientsPage
        .getZipCode()
        .clear()
        .type('123456789012345')
    merchantsPage
        .getNextBtn()
        .should('be.enabled')
        .click()
    recipientsPage
        .getThirthStep()
        .should('have.class','active')
    merchantsPage
      .getCheckboxValueOne()
      .should('not.be.checked')
    merchantsPage
      .getCheckboxValueTwo()
      .should('not.be.checked')
    merchantsPage
      .getCheckboxValueThree()
      .should('not.be.checked')
    merchantsPage
      .getCheckboxValueFour()
      .should('not.be.checked')
    merchantsPage
      .getCheckboxValueFife()
      .check()
      .should('be.checked')
    merchantsPage
      .getCheckboxValueSix()
      .should('not.be.checked')
    merchantsPage
      .getInputFdaCode()
      .should('be.disabled')
    recipientsPage
      .getSubmitBtn()
      .should('be.enabled')
      .click()
    merchantsPage 
      .getModal()
      .click()
    })
})