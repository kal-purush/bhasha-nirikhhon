import { Then } from 'cypress-cucumber-preprocessor/steps';


Then('search link with url {string} appears on page', (a) => {
  cy.get('a').should('have.attr', 'href');
  cy.get('a').invoke('attr','href').should('contain', a);
});

Then('search link contains text {string}', (a) => {
cy.get('a').should('have.text',a);
});