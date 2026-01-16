describe('Onboard -- User Journey', () => {
  before(() => {
    cy.visit('#/onboard')
  })

  it('User journey: Onboarding (Use existing)', () => {
    cy.percySnapshot('onboard_existing-wallet_home-1')
    cy.getByTestId('use-existing-wallet').click()
    cy.percySnapshot('onboard_existing-wallet_keys-2')
  })

  it('User journey: Onboarding (Recovery phrase)', () => {
    cy.percySnapshot('onboard_recovery-phrase_home-1')
    cy.getByTestId('import-wallet').click()
    cy.getByTestId('wallet-import-form-name').type('Test')
    cy.getByTestId('wallet-import-form-recovery-phrase').type(
      'Word '.repeat(24)
    )
    cy.getByTestId('wallet-import-form-passphrase').type('123')
    cy.getByTestId('wallet-import-form-passphrase-confirm').type('123')
    cy.percySnapshot('onboard_recovery-wallet_import-2')
    cy.getByTestId('wallet-import-form-submit').click()
    cy.percySnapshot('onboard_recovery-wallet_keys-3')
  })

  it('User journey: Onboarding (Create wallet)', () => {
    cy.percySnapshot('onboard_create-wallet_home-1')
    cy.getByTestId('create-new-wallet').click()

    cy.getByTestId('create-wallet-form-name').type('Test')
    cy.getByTestId('create-wallet-form-passphrase').type('123')
    cy.getByTestId('create-wallet-form-passphrase-confirm').type('123')
    cy.percySnapshot('onboard_create-wallet_form-2')

    cy.getByTestId('create-wallet-form-submit').click()
    cy.percySnapshot('onboard_create-wallet_success-3')

    cy.getByTestId('create-wallet-success-cta').click()
    cy.percySnapshot('onboard_create-wallet_keys-4')
  })
})