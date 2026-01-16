import { ACCOUNT_HOME_PATH } from '../../src/config/paths';
import {
  LOGIN_REQUIRED_BUTTON_ID,
  LOGIN_REQUIRED_TEXT_ID,
} from '../../src/config/selectors';
import { SIGN_IN_PATH } from '../support/server';

describe('Redirect when not logged in', () => {
  beforeEach(() => {
    cy.setUpApi({ currentMember: null });
    cy.visit(ACCOUNT_HOME_PATH);
  });

  it('redirects to the login page when not logged in', () => {
    cy.wait('@getCurrentMember');
    cy.get(`#${LOGIN_REQUIRED_TEXT_ID}`).should('be.visible');
    cy.get(`#${LOGIN_REQUIRED_BUTTON_ID}`).click();
    cy.wait('@signInRedirection');
    cy.url().should('contain', `${SIGN_IN_PATH}?url=`);
  });
});