describe('My First Test', () => {
  it('Visits the Landing Page', () => {
    cy.visit('/');
    cy.contains('Angular');
    cy.location('href').should('contain', 'landing');
  });

  it('Visits the Home Page and redirected to Auth0', () => {
    cy.visit('/home');
    cy.location('href').should('contain', 'auth0.com');
  });
});