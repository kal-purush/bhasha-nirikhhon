/// <reference types="Cypress" />

context('About', () => {
    beforeEach(() => {
        //
    })

    it('successfully loads about', () => {
        cy.visit('/about')
    })
})