import { SHARED_ITEMS_PATH } from '../../src/config/paths';
import {
  HOME_MENU_DROPDOWN_BUTTON_ID,
  HOME_MENU_OWN_MENUITEM_ID,
  ROOT_MENU_DROPDOWN_BUTTON_ID,
  ROOT_MENU_ID,
  buildBreadcrumbsItemLink,
  buildMenuDropdownButton,
  buildMenuItem,
} from '../../src/config/selectors';
import MOCK_ITEMS from '../../src/mockServer/mockData/items';

describe('Breadcrumbs', () => {
  beforeEach(() => {
    cy.setUpApi();
  });

  it('Home menu layout', () => {
    cy.visit('/');

    // menuitem should not exist before clicking the dropdown button
    cy.get(`#${HOME_MENU_OWN_MENUITEM_ID}`).should('not.exist');
    cy.get(`#${HOME_MENU_DROPDOWN_BUTTON_ID}`).click();

    // menuitem should be visible after clicking the dropdown button
    cy.get(`#${HOME_MENU_OWN_MENUITEM_ID}`).should('be.visible');
  });

  it('Navigate own items', () => {
    // The testing file structure is in the following
    // - My Items
    //  - folder1
    //    - folder2
    //      - document2
    //  - document1
    const FOLDER1_ID = MOCK_ITEMS[0].id;
    const FOLDER2_ID = MOCK_ITEMS[1].id;
    const DOCUMENT1_ID = MOCK_ITEMS[2].id;
    const DOCUMENT2_ID = MOCK_ITEMS[3].id;

    // Navigate to the folder1 by the dropdown menu
    cy.visit('/');
    cy.get(`#${ROOT_MENU_DROPDOWN_BUTTON_ID}`).click();
    cy.get(`#${buildMenuItem(FOLDER1_ID, ROOT_MENU_ID)}`)
      .should('be.visible')
      .click();
    cy.wait(2000);
    // Check the layout and the url
    cy.get(`#${buildBreadcrumbsItemLink(FOLDER1_ID)}`).should('be.visible');
    cy.get(`#${buildMenuDropdownButton(FOLDER1_ID)}`).should('be.visible');
    cy.url().should('include', `${FOLDER1_ID}`);

    // Navigate to the folder2 by the dropdown menu
    cy.get(`#${buildMenuDropdownButton(FOLDER1_ID)}`).click();
    cy.get(`#${buildMenuItem(FOLDER2_ID, FOLDER1_ID)}`)
      .should('exist')
      .and('be.visible')
      .click();
    cy.wait(2000);
    // Check the layout and the url
    cy.get(`#${buildBreadcrumbsItemLink(FOLDER2_ID)}`).should('be.visible');
    cy.get(`#${buildMenuDropdownButton(FOLDER2_ID)}`).should('be.visible');
    cy.url().should('include', `${FOLDER2_ID}`);

    // Navigate to the document2 by the dropdown menu
    cy.get(`#${buildMenuDropdownButton(FOLDER2_ID)}`).click();
    cy.get(`#${buildMenuItem(DOCUMENT2_ID, FOLDER2_ID)}`)
      .should('exist')
      .and('be.visible')
      .click();
    cy.wait(2000);
    // Check the layout and the url
    cy.get(`#${buildBreadcrumbsItemLink(FOLDER1_ID)}`).should('be.visible');
    cy.get(`#${buildBreadcrumbsItemLink(FOLDER2_ID)}`).should('be.visible');
    cy.get(`#${buildBreadcrumbsItemLink(DOCUMENT2_ID)}`).should('be.visible');
    cy.get(`#${buildMenuDropdownButton(DOCUMENT2_ID)}`).should('not.exist');
    cy.url().should('include', `${DOCUMENT2_ID}`);

    // Navigate back to the folder2 by the breadcrumbs item
    cy.get(`#${buildBreadcrumbsItemLink(FOLDER2_ID)}`).click();
    cy.wait(2000);
    // Check the layout and the url
    cy.get(`#${buildBreadcrumbsItemLink(FOLDER1_ID)}`).should('be.visible');
    cy.get(`#${buildBreadcrumbsItemLink(FOLDER2_ID)}`).should('be.visible');
    cy.get(`#${buildBreadcrumbsItemLink(DOCUMENT2_ID)}`).should('not.exist');
    cy.url().should('include', `${FOLDER2_ID}`);

    // Navigate to the document1 by the dropdown menu
    cy.get(`#${ROOT_MENU_DROPDOWN_BUTTON_ID}`).click();
    cy.get(`#${buildMenuItem(DOCUMENT1_ID, ROOT_MENU_ID)}`)
      .should('be.visible')
      .click();
    cy.wait(2000);
    // Check the layout and the url
    cy.get(`#${buildBreadcrumbsItemLink(DOCUMENT1_ID)}`).should('be.visible');
    cy.get(`#${buildBreadcrumbsItemLink(FOLDER1_ID)}`).should('not.exist');
    cy.get(`#${buildBreadcrumbsItemLink(FOLDER2_ID)}`).should('not.exist');
    cy.get(`#${buildBreadcrumbsItemLink(DOCUMENT2_ID)}`).should('not.exist');
    cy.url().should('include', `${DOCUMENT1_ID}`);
  });

  it('Navigate shared items', () => {
    // The testing file structure is in the following
    // - Shared Items
    //  - folder3
    //    - sharedFolder1
    //      - sharedDocument1
    const FOLDER3_ID = MOCK_ITEMS[4].id;
    const SHARED_FOLDER1_ID = MOCK_ITEMS[5].id;
    const SHARED_DOCUMENT1_ID = MOCK_ITEMS[6].id;

    // Navigate to the shared folder1 by the dropdown menu
    cy.visit(SHARED_ITEMS_PATH);
    cy.get(`#${ROOT_MENU_DROPDOWN_BUTTON_ID}`).click();
    cy.get(`#${buildMenuItem(SHARED_FOLDER1_ID, ROOT_MENU_ID)}`)
      .should('be.visible')
      .click();
    cy.wait(2000);
    // Check the layout and the url
    cy.get(`#${buildBreadcrumbsItemLink(FOLDER3_ID)}`).should('not.exist');
    cy.get(`#${buildBreadcrumbsItemLink(SHARED_FOLDER1_ID)}`).should(
      'be.visible',
    );
    cy.url().should('include', `${SHARED_FOLDER1_ID}`);

    // Navigate to the shared document1 by the dropdown menu
    cy.get(`#${buildMenuDropdownButton(SHARED_FOLDER1_ID)}`).click();
    cy.get(`#${buildMenuItem(SHARED_DOCUMENT1_ID, SHARED_FOLDER1_ID)}`)
      .should('exist')
      .and('be.visible')
      .click();
    cy.wait(2000);
    // Check the layout and the url
    cy.get(`#${buildBreadcrumbsItemLink(FOLDER3_ID)}`).should('not.exist');
    cy.get(`#${buildBreadcrumbsItemLink(SHARED_FOLDER1_ID)}`).should(
      'be.visible',
    );
    cy.get(`#${buildBreadcrumbsItemLink(SHARED_DOCUMENT1_ID)}`).should(
      'be.visible',
    );
    cy.url().should('include', `${SHARED_DOCUMENT1_ID}`);

    // Navigate to the shared folder1 by the breadcrumbs item
    cy.get(`#${buildBreadcrumbsItemLink(SHARED_FOLDER1_ID)}`).click();
    cy.wait(2000);
    // Check the layout and the url
    cy.get(`#${buildBreadcrumbsItemLink(FOLDER3_ID)}`).should('not.exist');
    cy.get(`#${buildBreadcrumbsItemLink(SHARED_FOLDER1_ID)}`).should(
      'be.visible',
    );
    cy.url().should('include', `${SHARED_FOLDER1_ID}`);
  });
});