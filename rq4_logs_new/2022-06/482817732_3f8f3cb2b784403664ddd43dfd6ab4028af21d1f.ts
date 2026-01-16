import {Given, When, Then, And} from 'cypress-cucumber-preprocessor/steps';
import { DataTable } from '@cucumber/cucumber';

Given('I\'m in the homepage', () => {
  cy.visit('http://localhost:4200/degrees/1');
});

Given('I click on nav item {string}', (value) => {
  cy.get('.card-text').contains(value).click();
  cy.wait(1000);
});

When('The form is filled with email name {string}',
  (name) => {
    cy.get('#email').should('have.value', name);
});

When('I clear and fill the form with', (table: DataTable) => {
  table.rows().forEach((pair: string[]) =>
  cy.get('#' + pair[0]).clear().type(pair[1]).blur() );
});

When('I clear the {string} button', (value) => {
  cy.get('#email' + value).clear();
  cy.wait(1000);
});

When('I click the {string} checkbox', (value) => {
  cy.get('#' + value).click();
  cy.wait(1000);
});

Then('I see the admin page with email {string}', (value) => {
  cy.get('#email').contains(value).should('exist');
});
