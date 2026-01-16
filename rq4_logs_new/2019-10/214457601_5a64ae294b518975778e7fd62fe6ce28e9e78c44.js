import { Given, When, Then } from "cypress-cucumber-preprocessor/steps";

Given('the user access the event {string}', (url) => {
    cy.viewport(1920, 1080);
    cy.visit(url);
    try {
        cy.get('.c01202')
        .click();
    } catch (error) {
        cy.log('Não foi possível fechar o frame');
    }
})

When('the user clicks on the button {string}', (button) => {
    cy.contains(button)
        .click();
    cy.wait(3000);
})

Then('should be displayed the tickets selection window', () => {
    //cy.contains('Carregando ingressos...').should('not.visible');
    cy.contains('Ingressos').should('be.visible');
    cy.contains('Usar cupom promocional').should('be.visible');
    cy.get('.c01216.c01217').should(($buttons) => {
        expect($buttons).to.have.length(2)
      })
    cy.contains('Ingresso gratuito').should('be.visible');
    cy.contains('Ingresso pago').should('be.visible');
    cy.get('.c0135.c0137.c01213').should(($valor) => {
        expect($valor).to.have.length(2);
      })
    cy.contains('0 ingresso').should('be.visible');
})