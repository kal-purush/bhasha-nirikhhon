/// <reference types="Cypress"/>

import { Given, When, Then } from 'cypress-cucumber-preprocessor/steps';
import bodyTokenAuth01 from '../../support/payloads/tokenAuthBasicUser';
import bodyTokenAuth02 from '../../support/payloads/tokenAuthManagerUser';
import apiTokenGeneral from '../services/tokenAuth';
import apiTokenOperation from '../services/tokenOperation';

let bodyCalled;

//Scenario: Authentication - mobile Basic User
Given('already route for a user with {string}', (string1) => {
    if (string1.indexOf('positive scenario')!=-1){
        bodyCalled = bodyTokenAuth01;
    }
    if (string1.indexOf('negative scenario')!=-1){
        bodyCalled = bodyTokenAuth02;
    }
    cy.log(Cypress.env('tokenAuth'));
    cy.log('ENV=' + Cypress.env('baseEnv'));
});

When('execute a basic request', () => {

    apiTokenGeneral.callGetTokenServiceUser(bodyCalled).its('body.token').should('not.be.empty')
    .then(token => apiTokenOperation.callGetToken(token))
    .as('sSpice245')
});

Then('the API response will return with the {string}', (string2) => {
    cy.get('@sSpice245').should((response) => {
        var jsonData = JSON.stringify(response.body)
        expect(jsonData).to.have.string(string2)
    })
});

//Scenario: Authentication - mobile Manager User
Given('already route for a manager user with {string}', (string1) => {
    if (string1.indexOf('positive scenario')!=-1){
        bodyCalled = bodyTokenAuth01;
    }
    if (string1.indexOf('negative scenario')!=-1){
        bodyCalled = bodyTokenAuth02;
    }
    cy.log(Cypress.env('tokenAuth'));
    cy.log('ENV=' + Cypress.env('baseEnv'));
});

When('execute a special request', () => {
    apiTokenGeneral.callGetTokenServiceManager(bodyCalled).as('sSpice246')
});

Then('the API response will return with the dashboard {string}', (string2) => {
    cy.get('@sSpice246').should((response) => {
        var jsonData = JSON.stringify(response.body)
        expect(jsonData).to.have.string(string2)
    })
});