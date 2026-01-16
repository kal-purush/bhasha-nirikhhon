beforeEach(() => {
  cy.visit('/courseAdmin')
  cy.wait(2000)
})

describe('Course admin page tests', () => {
  it('Shows all courses', () => {
    cy.get('.chapter').should('have.length', 3)
  })

  it('Expands course data on header click', () => {
    cy.get('.chapter').first().click()
    cy.get('.chapter-content').first().should('be.visible')
  })

  it('Opens new instance form, when button is pressed', () => {
    cy.get('.newCourseButton').first().click()
    cy.get('.newInstanceForm-visible').first().should('be.visible')
  })

  it('form disappears when close button is pressed', () => {
    cy.get('.newCourseButton').first().click()
    cy.get('.newInstanceForm-visible').first().should('be.visible')
    cy.get('.newCourseButton').first().click()
    cy.get('.newInstanceForm-hidden').first().should('not.be.visible')
  })

  it('Shows error when submitting empty form', () => {
    cy.get('.newCourseButton').first().click()
    cy.get('[type="submit"]').first().click()
    cy.get('.global_error-visible').first().should('be.visible')
  })
})