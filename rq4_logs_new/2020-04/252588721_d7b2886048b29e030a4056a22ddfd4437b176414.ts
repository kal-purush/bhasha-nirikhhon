describe("Sandbox", () => {
  before(() => {
    cy.visit("https://e2e-boilerplate.github.io/sandbox/");
  });

  it("should be on Sandbox", async () => {
    cy.title().then((title) => {
      expect(title).to.eq("Sandbox");
    });
    cy.get("h1").then((header) => {
      expect(header).to.eq("Sandbox");
    });
  });
});