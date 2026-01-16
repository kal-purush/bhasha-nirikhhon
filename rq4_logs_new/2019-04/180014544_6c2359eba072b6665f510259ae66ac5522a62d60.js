

const url = 'http://md5.jsontest.com/?text=foo'
/// <reference types="cypress" />
// @ts-check

context('custom command example', () => {
    it(' test original value', () => {
        cy.md5(url)
        .should("deep.eq", {
            md5: "acbd18db4cc2f85cedef654fccc4a4d8",
            original: "foo"
          });
    })
})