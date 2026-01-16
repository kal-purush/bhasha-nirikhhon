describe('example to-do app', () => {
  it('successfully loads', () => {
    cy.visit('http://localhost:3000') // change URL to match your dev URL
  })


  it("should load product details page", () => {
    cy.get(".products article").first().should("exist").click({ multiple: true });
    cy.url().should("include", "/products/");
  });

})