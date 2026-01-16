import { ImportWordListPage } from 'cypress/support/import-wordlist.po';

describe('Import Word List', () => {
    const page = new ImportWordListPage();

    before(() => {
        cy.task('seed:database');
      });

      beforeEach(() => {
        page.navigateTo();
      });

      it('should not enable the import button', () => {
          page.importButton().should('be.disabled');
      });
      it('should enable the cancel button', () => {
          page.cancelImportButton().should('be.enabled');
      });
});