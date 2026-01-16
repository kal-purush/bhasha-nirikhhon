import {Given, Then} from 'cypress-cucumber-preprocessor/steps';

Given('I click on nav item {string}', (value) => {
  cy.get('.nav-link').contains(value).click();
});

Given('I click on dropdown item {string}', (value) => {
  cy.get('.dropdown-item').contains(value).click();
});

Then('I see the data of the subject with name {string}, course {string} and optional {string}',
  (subjectName, subjectCourse, subjectOptional) => {
  cy.get('#subjectName').contains(subjectName).should('exist');
  cy.get('#subjectCourse').contains(subjectCourse).should('exist');
  cy.get('#subjectOptional').contains(subjectOptional).should('exist');
  });

Then('The button {string} does not exists', (value) => {
  cy.get('button').contains(value).should('not.exist');
});
