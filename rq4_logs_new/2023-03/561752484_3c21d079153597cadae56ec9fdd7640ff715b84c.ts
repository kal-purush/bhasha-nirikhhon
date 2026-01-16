const appConfigMock = {
  logLevel: 'debug',
  vegaHome: '',
  defaultNetwork: 'test',
  telemetry: {
    consentAsked: false,
    enabled: true,
  },
}

describe('Telemetry dialog elements', () => {
  // 0001-WALL-003 if the app sends telemetry/analytics: must be prompted to opt into (or stay out of) analytics
  before(() => {
    cy.mock('service.GetAppConfig', appConfigMock)
    cy.visit('/')
  })
  it('should see telemetry dialog', () => {
    cy.get('[role="dialog"]').should('be.visible')
  })
  it('should see telemetry dialog title', () => {
    cy.get('[role="dialog"] h2')
      .should('be.visible')
      .and('have.text', 'Report bugs and crashes')
  })

  it('should see info text', () => {
    cy.get('[data-testid="telemetry-option-form"] > p').should(
      'have.text',
      'Selecting yes will help developers improve the software'
    )
  })

  it('should see form options', () => {
    cy.get('[data-testid="telemetry-option-form"] [role="radio"]').then(
      ($radio) => {
        expect($radio).to.have.length(2)
        expect($radio.eq(0)).to.have.value('no')
        expect($radio.eq(0)).to.have.attr('data-state', 'checked')
        expect($radio.eq(1)).to.have.value('yes')
        expect($radio.eq(1)).to.have.attr('data-state', 'unchecked')
      }
    )
  })

  it('should see form button', () => {
    cy.get('[data-testid="telemetry-option-form"] button').should(
      'have.text',
      'Continue'
    )
  })
})

describe('Telemetry dialog action', () => {
  before(() => {
    cy.mock('service.GetAppConfig', appConfigMock)
    cy.visit('/')
  })
  it('should be able to submit telemetry form', () => {
    cy.get('[data-testid="telemetry-option-form"] #yes').click()
    cy.get('[data-testid="telemetry-option-continue"]').click()
    cy.get('[role="dialog"]').should('not.be.visible')
  })
})