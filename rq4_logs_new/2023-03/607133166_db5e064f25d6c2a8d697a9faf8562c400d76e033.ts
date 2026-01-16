describe("Navigation", () => {
  beforeEach(() => {
    cy.visit("/");
  });

  it("should check for the text Hello World", () => {
    cy.get("h1").should("contain", "Hello World!");
  });

  it("should navigate to the about page", () => {
    // Find a link with an href attribute containing "about" and click it
    cy.get('a[href*="about"]').click();

    // The new url should include "/about"
    cy.url().should("include", "/about");

    // The new page should contain an h1 with "About page"
    cy.get("h1").contains("About Page");
  });
});

export {};