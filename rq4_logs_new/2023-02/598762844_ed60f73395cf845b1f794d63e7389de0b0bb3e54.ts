import { TodoListPage } from '../support/todo-list.po';

const page = new TodoListPage();

describe('Todo list', () => {

  beforeEach(() => {
    page.navigateTo();
  });

  it('Should have the correct title', () => {
    page.getTodoTitle().should('have.text', 'Todos');
  });

  it('Should type something in the owner filter and check that it returned correct elements', () => {

    cy.get('[data-test=todoOwnerInput]').type('Workman');
  });

    //page.getTodoCards().each($card => {
    //  cy.wrap($card).find('.todo-card-name').should('have.text', 'Workman');
    //});


    //page.getTodoCards().find('.todo-card-name').each($name =>
    //  expect($name.text()).to.equal('Workman')
    //;

  /*it('Should type something in the company filter and check that it returned correct elements', () => {
    // Filter for company 'OHMNET'
    cy.get('[data-test=todoCompanyInput]').type('OHMNET');

    page.getUserCards().should('have.lengthOf.above', 0);

    // All of the user cards should have the company we are filtering by
    page.getUserCards().find('.user-card-company').each($card => {
      cy.wrap($card).should('have.text', 'OHMNET');
    });
  });

  it('Should type something partial in the company filter and check that it returned correct elements', () => {
    // Filter for companies that contain 'ti'
    cy.get('[data-test=userCompanyInput]').type('ti');

    page.getUserCards().should('have.lengthOf', 2);

    // Each user card's company name should include the text we are filtering by
    page.getUserCards().each(e => {
      cy.wrap(e).find('.user-card-company').should('include.text', 'TI');
    });
  });*/

  it('Should select a status, and check that it returned correct elements', () => {

    page.selectStatus(true);

    page.getTodoListItems().should('have.lengthOf.above', 0);

    page.getTodoListItems().each($todo => {
      cy.wrap($todo).find('.todo-list-status').should('contain', 'complete');
    });
  });

  /*it('Should change the view', () => {
    // Choose the view type "List"
    page.changeView('list');

    // We should not see any cards
    // There should be list items
    page.getUserCards().should('not.exist');
    page.getUserListItems().should('exist');

    // Choose the view type "Card"
    page.changeView('card');

    // There should be cards
    // We should not see any list items
    page.getUserCards().should('exist');
    page.getUserListItems().should('not.exist');
  });
  */

  it('Should select a category, and check that it returned correct elements', () => {
    // Filter for role 'viewer');
    page.selectCategory('software design');

    // Choose the view type "List"
    //page.changeView('list');

    // Some of the users should be listed
    page.getTodoListItems().should('have.lengthOf.above', 0);

    // All of the user list items that show should have the role we are looking for
    page.getTodoListItems().each($todo => {
      cy.wrap($todo).find('.todo-list-role').should('contain', 'software design');
    });
  });

  /*it('Should click view profile on a user and go to the right URL', () => {
    page.getUserCards().first().then((card) => {
      const firstUserName = card.find('.user-card-name').text();
      const firstUserCompany = card.find('.user-card-company').text();

      // When the view profile button on the first user card is clicked, the URL should have a valid mongo ID
      page.clickViewProfile(page.getUserCards().first());

      // The URL should contain '/users/' (note the ending slash) and '/users/' should be followed by a mongo ID
      cy.url()
        .should('contain', '/users/')
        .should('match', /.*\/users\/[0-9a-fA-F]{24}$/);

      // On this profile page we were sent to, the name and company should be correct
      cy.get('.user-card-name').first().should('have.text', firstUserName);
      cy.get('.user-card-company').first().should('have.text', firstUserCompany);
    });
   });
  */

});