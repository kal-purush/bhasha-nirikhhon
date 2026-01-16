import {LoginPage} from './page-object/LoginPage'
import HomePage from './page-object/HomePage'
const email = Cypress.env('email')
const password = Cypress.env('password')
describe('Log in on page and check inputs', () => {

  it('Should log in the page via  english language', () => {
    const home = new HomePage();
    home.visit();
   //visit log in page
   const logIn = new LoginPage()
   logIn.visit()
   //write invalid email
   logIn.getEmail('diana@gmailcom')
   cy.contains('This field must be a valid email address')
    //write invalid password
   logIn.getPassword('1')
   logIn.getCheckbox().click()
  // cy.contains('This field is required')
   logIn.getLoginBtn().should('be.disabled')
   //write invalid email
   logIn.getEmail('dianagmail.com')
   cy.contains('This field must be a valid email address')
    //write invalid password
   logIn
    .getPassword('12344567')
    .getCheckbox().click()
   logIn.getLoginBtn().should('be.disabled')
   //write valid email
   logIn
    .getEmail(email)
    //write valid password
    .getPassword(password)
    .getCheckbox().click()
   //click login
   logIn.getLoginBtn().click()
   //check base url
   cy.url().should('include','https://dev-login.priornotify.com/')
  })
  it('Should log in the page via  ukrainian language', () => {
    const home = new HomePage();
    home.visit();
   //visit log in page
   const logIn = new LoginPage()
   logIn.visit()
    logIn
      .getLanguage()
      .click()
    cy.getIframeBody('.goog-te-menu-frame.skiptranslate')
      .last()
      .find('.goog-te-menu2 table tbody span:nth-child(2).text')
      .contains('українська')
      .click()
    cy.wait(2000)
    //write invalid email
    logIn
      .getEmail('1@gmail.com')
     //write invalid password
      .getPassword('1234567')
      .getCheckbox()
      .click()
    logIn.getLoginBtn()
    //write invalid email
    logIn
      .getEmail('діана')
    cy.contains('Це поле має бути дійсною електронною адресою')
     //write invalid password
    logIn
      .getPassword('12344567')
      .getLoginBtn()
      .should('be.disabled')
    //write valid email
    logIn
    .getEmail(email)
    //write valid password
      .getPassword(password)
    //click login
      .getLoginBtn()
      .click()
    //check base url
    cy.url().should('include','https://dev-login.priornotify.com/')
   })
})