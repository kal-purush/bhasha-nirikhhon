beforeEach(() => {
  cy.visit('/')
  cy.wait(2000)
})

describe('Course page tests', () => {
  it('Displays all tabs for course chapters', () => {
    cy.get('.course-title').first().click()
    cy.get('.containerwrapper > .white-link').its('length').should('eq', 6)
  })

  it('Should change tabs on clicking', () => {
    cy.get('.course-title').first().click()

    cy.get('.containerwrapper > .white-link').eq(1).click()
    cy.contains('Rationaaliluvut')
  })

  it('Should expand boxes when box is clicked', () => {
    cy.get('.course-title').first().click()
    cy.get('.chapter').first().click()

    cy.get('.chapter-content').first().should('be.visible')
  })

  it('Should expand boxes when box is clicked 2', () => {
    cy.get('.course-title').first().click()
    cy.scrollTo('center')
    cy.get('.chapter').eq(1).click()
    cy.get('.chapter-content').eq(1).should('be.visible')
  })

  it('Should close boxes when opened box header is clicked', () => {
    cy.get('.course-title').first().click()
    cy.get('.chapter').eq(1).click()
    cy.get('.chapter-content').eq(1).should('be.visible')
    cy.get('.chapter').eq(1).click()
    cy.get('.chapter-content').eq(1).should('not.be.visible')
  })

  it('Should close boxes when close button is pressed', () => {
    cy.get('.course-title').first().click()
    cy.get('.chapter').first().click()
    cy.get('.chapter-content').first().should('be.visible')
    cy.get('.close-chapter').first().click()
    cy.get('.chapter-content').first().should('not.be.visible')
  })

  it('Should open exercies when box is clicked', () => {
    cy.get('.course-title').first().click()
    cy.get('.chapter').eq(1).click()
    cy.get('.chapter-content').eq(1).should('be.visible')
    cy.get('.exercise').first().click()
    cy.get('.exercise-content').first().should('be.visible')
  })

  it('Should close exercies when box is clicked', () => {
    cy.get('.course-title').first().click()
    cy.get('.chapter').eq(1).click()
    cy.get('.chapter-content').eq(1).should('be.visible')
    cy.get('.exercise').first().click()
    cy.get('.exercise-content').first().should('be.visible')
    cy.get('.exercise').first().click()
    cy.get('.exercise-content').first().should('not.be.visible')

  })
})