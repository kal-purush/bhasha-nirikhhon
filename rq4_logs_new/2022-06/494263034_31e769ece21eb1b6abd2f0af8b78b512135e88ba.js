/// <reference types="cypress" />


describe('Teste funcionais', () => {
    beforeEach(() => {
        cy.visit('http://automationpractice.com/index.php')
    })

    it('Making a Purchase', function () {
        cy.fixture('userData').as('usuario').then(() => {
            cy.get('#search_query_top').type('Printed Dress')
            cy.get('#searchbox > .btn').click()
            cy.get(':nth-child(2) > .product-container > .right-block > [itemprop="name"] > .product-name')
                .click()
            cy.get('#group_1').select('M')
            cy.get('.exclusive > span').click()
            cy.get('.button-medium > span').click()
            cy.get('[title="View my shopping cart"]').should('have.length', 1)
            cy.get('[title="View my shopping cart"]').click()
            cy.get('.cart_navigation > .button > span').click()
            cy.get('#email').type(this.usuario.email)
            cy.get('#passwd').type(this.usuario.senha)
            cy.get('#SubmitLogin > span').click()
            cy.get('.cart_navigation > .button > span').click()
            cy.get('#cgv').click()
            cy.get('.cart_navigation > .button > span').click()
            cy.get('.bankwire').click()
            cy.get('#cart_navigation > .button > span').click()

            cy.get('.cheque-indent > .dark').should('contain', 'Your order on My Store is complete.')
        })
    })
})