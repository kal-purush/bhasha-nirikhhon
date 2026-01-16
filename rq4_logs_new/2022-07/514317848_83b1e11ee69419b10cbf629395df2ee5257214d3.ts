export function loadInterceptsDefaults(){
    cy.intercept('POST',/\/auth\/activate/, { fixture: 'interceptActivateFixtures.json' }).as('PostActivate-Fixtures')
    cy.intercept('GET',/\/users\/current/,{fixture: 'interceptCurrentFixtures.json'}).as('GetCurrent-Fixtures')
    cy.intercept('POST',/\/users\/type/, { fixture: 'interceptTypeFixtures.json' }).as('PostType-Fixtures')
    cy.intercept('POST',/\/users\/subscribe/, { fixture: 'interceptSubscribeFixtures.json' }).as('PostSubscribe-Fixtures')
}