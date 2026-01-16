import {And, Given, Then, When} from 'cypress-cucumber-preprocessor/steps';

When('I select the object type {string}', (value) => {
  cy.get('select').select(value);
  cy.wait(1200);
});

When('I fill the {string} input with {string}', (id, value) => {
  cy.get('#' + id).type(value);
});

Then('In the list I see the data of the university with name {string}, acronym {string}, city {string} and country {string}',
  (uniName, uniAcronym, uniCity, uniCountry) => {
    cy.get('.universityName').contains(uniName).should('exist');
    cy.get('.universityAcronym').contains(uniAcronym).should('exist');
    cy.get('.universityCity').contains(uniCity).should('exist');
    cy.get('.universityCountry').contains(uniCountry).should('exist');
  });

Then('There are no elements on the list', () => {
  cy.get('.card').should('not.exist');
});

