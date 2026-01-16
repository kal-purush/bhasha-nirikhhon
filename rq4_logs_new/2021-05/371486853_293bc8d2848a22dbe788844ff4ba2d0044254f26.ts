
describe('first test', () => {
  it('test with one tag',
    // @ts-ignore
    { tags: '@flaky' }
    , () => {
      cy.visit('/')
      cy.log('This test should run when cli command is --grep=@flaky')
    })

  it('test without tags', () => {
    cy.visit('/')
    cy.log('This test should run when cli command is --grep=-@flaky')
  })

  it('test with 2 tags',
    // @ts-ignore
    { tags: ['@flaky', '@bug'] }, () => {
      cy.visit('/')
      cy.log('This test should run when cli command is --grep=-@flaky')
    })

})