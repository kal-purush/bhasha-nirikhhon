/// <reference types="cypress" />

const viewports: Cypress.ViewportPreset[] = [
  "iphone-5",
  "iphone-8",
  "iphone-x",
  "samsung-s10",
  "ipad-2",
  "macbook-13",
  "macbook-15",
];

viewports.forEach((viewport) => {
  describe(`index (${viewport})`, () => {
    beforeEach(() => {
      cy.viewport(viewport);
      cy.visit("http://localhost:3000");
    });

    it("displays my name and story", () => {
      cy.contains("Jean-Philippe Roy").should("be.visible");

      cy.get('[data-test-id="paragraph-1"]').should("exist");
      cy.get('[data-test-id="paragraph-2"]').should("exist");
      cy.get('[data-test-id="paragraph-3"]').should("exist");
    });

    it("displays my avatar", () => {
      cy.get('[data-test-id="avatar"]').should("be.visible");
    });

    it("displays social icons", () => {
      cy.get('[data-test-id="linkedin"]').should("be.visible");
      cy.get('[data-test-id="github"]').should("be.visible");
    });

    it("allows the user to switch themes", () => {
      cy.get("html").should("not.have.class", "dark");

      cy.get(`[data-test-id="theme-switch"]`)
        .should("be.visible")
        .should("have.text", "🦉")
        .click()
        .should("have.text", "🦚");

      cy.get("html").should("have.class", "dark");
    });
  });
});