describe('Interactions', () => {
  before(() => {
    cy.visit('/')
  })
  it('should display unhealthy status', () => {
    cy.visit('/')
    cy.window().then((win) => {
      win.document.body.dispatchEvent(new CustomEvent('service_is_unhealthy'))
    })
    cy.get('[data-testid="service-status-unhealthy"]').should(
      'have.text',
      'Wallet Service: Unhealthy Restart'
    )
  })

  it('should display healthy status', () => {
    cy.visit('/')
    cy.window().then((win) => {
      win.document.body.dispatchEvent(new CustomEvent('service_is_healthy'))
    })
    cy.get('[data-testid="service-status"]').should(
      'have.text',
      'Wallet Service: test on http://localhost:1789'
    )
  })

  it('should display unreachable status', () => {
    cy.visit('/')
    cy.window().then((win) => {
      win.document.body.dispatchEvent(new CustomEvent('service_unreachable'))
    })
    cy.get('[data-testid="service-status-unreachable"]').should(
      'have.text',
      'Wallet Service: Not reachable, retrying'
    )
  })

  it('should display service stopped with error status', () => {
    cy.visit('/')
    cy.window().then((win) => {
      win.document.body.dispatchEvent(
        new CustomEvent('service_stopped_with_error')
      )
    })
    cy.get('[data-testid="service-status-error"]').should(
      'have.text',
      'Wallet Service: Failed to start. Try to restart'
    )
  })

  it('should display service stopped status', () => {
    cy.visit('/')
    cy.window().then((win) => {
      win.document.body.dispatchEvent(new CustomEvent('service_stopped'))
    })
    cy.get('[data-testid="service-status-stopped"]').should(
      'have.text',
      'Wallet Service: Not running. Start service'
    )
  })
})