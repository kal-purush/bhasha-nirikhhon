/// <reference types="cypress" />

describe("Landing Component", () => {
  beforeEach(() => {
    cy.visit("/"); // Assuming the component is rendered on the landing page
  });

  it('should display the "Who We Are" section with the correct content', () => {
    cy.contains("Who We Are");
    cy.contains("A2SV upskills high-potential university students");
    cy.contains(
      "The program is free for students, making the opportunity available for youth who have talent but lack the means to use it."
    );
  });

  it('should navigate to the signup page when "Join Us" button is clicked', () => {
    cy.contains("Join Us").click();
    cy.url().should("include", "/auth/signup"); // Assuming the signup page URL contains '/auth/signup'
  });

  it('should navigate to the section with more information when "More Information" button is clicked', () => {
    cy.get('button:contains("More Information")').click();
    // Assuming you have a specific section with an id of 'more'
    cy.get("#more").should("exist");
  });

  it('should not display the "Join Us" button if user is authenticated', () => {
    // Mock the authenticated state
    cy.intercept("GET", "/api/auth", { isAuthenticated: true });

    cy.reload(); // Assuming the component needs to be re-rendered after mocking the state

    cy.contains("Join Us").should("not.exist");

    cy.intercept("GET", "/api/auth", { isAuthenticated: false });
  });
  it('should display the "Welcome" section with the correct content', () => {
    cy.contains("Welcome to the A2SV Community");
    cy.contains("We are strategists, designers and developers.");
    cy.contains(
      "Small enough to be simple and quick, but big enough to deliver the scope you want at the pace you need."
    );
  });

  it("should display two images in the grid", () => {
    cy.get(".grid-cols-1 > div").should("have.length", 7); // Assuming there are two <div> elements within the grid
    cy.get(".grid-cols-1 > div > img").should("have.length", 3); // Assuming there are two <img> elements within the <div> elements
  });

  it('should display the "How do I join A2SV?" section with the correct content', () => {
    cy.contains("How do I join A2SV?");
    cy.contains("The way to join A2SV is through A2SV Community Portal.");
  });

  it("should display the steps with icons and descriptions", () => {
    cy.contains("Sign Up");
    cy.contains(
      "Sign up to access contests and be eligible for applying to A2SV"
    );
    cy.contains("Take Contests");
    cy.contains("Access contests and take at least 2 contests consecutively");
    cy.contains("Apply");
    cy.contains("Complete your profile and apply to join A2SV");
  });
  it("should display the section title and description", () => {
    cy.contains("These and other companies partnered with us");
  });

  it("should display the partner company logos", () => {
    cy.get(" #partners > div")
      .should("have.length", 5) // Assuming there are 5 partner images
      .each(($partnerLogo) => {
        cy.wrap($partnerLogo).find("img").should("be.visible");
      });
  });
  it('should display the "Get Started" section', () => {
    cy.contains("Ready To Grow?");
    cy.contains("Are you ready to take the next step in your journey?");
    cy.contains("Embrace the limitless possibilities and join A2SV today.");
    cy.contains(
      "Together, let's unlock your true potential and create a brighter future through technology."
    );
  });

  it('should navigate to the signup page when "Get Started" button is clicked', () => {
    cy.contains("Get Started").click();
    cy.url().should("include", "/auth/signup"); // Assuming the signup page URL contains '/auth/signup'
  });

  it('should not display the "Get Started" button if user is authenticated', () => {
    // Mock the authenticated state
    cy.intercept("GET", "/api/auth", { isAuthenticated: true });

    cy.reload(); // Assuming the component needs to be re-rendered after mocking the state

    cy.contains("Get Started").should("not.exist");
  });
});