import { LoginPage } from '../pages/login.page';
import { TestSuitesListPage } from '../pages/test-suites-list.page';

const loginPage = new LoginPage();
const testSuiteListPage = new TestSuitesListPage();
const username = Cypress.env("username");
const password = Cypress.env("password");
const second_username = Cypress.env("second_username");
const fourth_username = Cypress.env("fourth_username");

function loginActions(username: string, password: string): void {
  loginPage.enterUsername(username);
  loginPage.enterPassword(password);
  loginPage.clickLogin();
}

describe('Login functionality', () => {
  beforeEach(() => {
    loginPage.navigate("/");
  })

  it('user can see error message when entered incorrect credentials', () => {
    loginActions(username, "Wrongpassword");
    loginPage.find(loginPage.VALIDATION_MSG).contains('li', '- incorrect username or password').should("be.visible");
  })


  it('non-admin user can succesfully login with correct credentials', () => {
    loginActions(username, password);
    loginPage.find(loginPage.LOGOUT_BUTTON);
  })



describe('Multiple accounts support', () => {
  beforeEach(() => {
    cy.setToken(second_username, password);
    cy.visit("/test-suites");
  })

  it('user can succesfully logout', () => {
    cy.get(loginPage.LOGOUT_BUTTON).click();
    cy.get(loginPage.SUBMIT_BUTTON);
  })

  it('user can click an account and be redirected to /test-suites for that account', () => {
    cy.getAccount(second_username, password).then((account) => {
      loginPage.find(loginPage.ACCOUNT_SELECT_DROPDOWN);
      const accountId = account.body.data.me.accounts.nodes[1].id;
      loginPage.find(loginPage.SELECT_OPTIONS).select(accountId);
      loginPage.find(testSuiteListPage.INTRO_PAGE);
    })

  })
})

