declare namespace Cypress {
  interface Chainable {
    /**
     * Custom comand to select DOM element by data-cy attribute.
     * @example cy.getByTestId('geeting')
     */
    getByTestId: (value: string) => Chainable<Element>;
  }
}